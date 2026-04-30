# Registry Operations

How the plugin registry works and how to set one up.

## Architecture

```
Push to branch → GitHub Actions → SDK CLI packages plugins → S3 hosts registry
```

- **S3 bucket** serves `index.json` and `artifacts/*.tar.gz` over public HTTPS
- **GitHub Actions** runs `scripts/publish.sh` on every push to the target branch
- **SDK CLI** (`influxdb3-plugin package`) does all validation, packaging, hashing, and index manipulation
- **publish.sh** is a dumb loop — fetches index, calls the CLI for each plugin dir, uploads results

## S3 Bucket Setup

```bash
BUCKET="influxdb3-plugins-registry"
REGION="us-east-2"
PROFILE="your-aws-profile"

# Create
aws s3api create-bucket --bucket $BUCKET --region $REGION \
  --create-bucket-configuration LocationConstraint=$REGION --profile $PROFILE

# Disable ACLs
aws s3api put-bucket-ownership-controls --bucket $BUCKET \
  --ownership-controls 'Rules=[{ObjectOwnership=BucketOwnerEnforced}]' --profile $PROFILE

# Enable versioning (rollback safety net)
aws s3api put-bucket-versioning --bucket $BUCKET \
  --versioning-configuration Status=Enabled --profile $PROFILE

# Enable encryption
aws s3api put-bucket-encryption --bucket $BUCKET \
  --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}' --profile $PROFILE

# Allow bucket policy (required before setting public read)
aws s3api put-public-access-block --bucket $BUCKET \
  --public-access-block-configuration \
  '{"BlockPublicAcls":true,"IgnorePublicAcls":true,"BlockPublicPolicy":false,"RestrictPublicBuckets":false}' \
  --profile $PROFILE

# Public read (GetObject only, no listing)
aws s3api put-bucket-policy --bucket $BUCKET --policy "{
  \"Version\":\"2012-10-17\",
  \"Statement\":[{
    \"Sid\":\"PublicReadGetObject\",\"Effect\":\"Allow\",
    \"Principal\":\"*\",\"Action\":\"s3:GetObject\",
    \"Resource\":\"arn:aws:s3:::${BUCKET}/*\"
  }]
}" --profile $PROFILE
```

### Seed the Index

```bash
influxdb3-plugin new index registry-guide \
  --artifacts-url "https://${BUCKET}.s3.${REGION}.amazonaws.com/artifacts"

aws s3 cp registry-guide/index.json s3://${BUCKET}/index.json \
  --content-type "application/json" --profile $PROFILE

# Verify
curl -sS "https://${BUCKET}.s3.${REGION}.amazonaws.com/index.json"
```

## GitHub Actions OIDC + IAM

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --profile $PROFILE)

# OIDC provider (skip if already exists org-wide)
aws iam create-open-id-connect-provider \
  --url "https://token.actions.githubusercontent.com" \
  --client-id-list "sts.amazonaws.com" \
  --thumbprint-list "6938fd4d98bab03faadb97b34396831e3780aea1" --profile $PROFILE

# Policy
aws iam create-policy --policy-name "${BUCKET}-publish" --policy-document "{
  \"Version\":\"2012-10-17\",
  \"Statement\":[
    {\"Effect\":\"Allow\",\"Action\":[\"s3:GetObject\",\"s3:PutObject\"],\"Resource\":\"arn:aws:s3:::${BUCKET}/*\"},
    {\"Effect\":\"Allow\",\"Action\":\"s3:GetObject\",\"Resource\":\"arn:aws:s3:::${BUCKET}\"}
  ]
}" --profile $PROFILE

# Role (update repo name as needed)
aws iam create-role --role-name "${BUCKET}-ci" --assume-role-policy-document "{
  \"Version\":\"2012-10-17\",
  \"Statement\":[{
    \"Effect\":\"Allow\",
    \"Principal\":{\"Federated\":\"arn:aws:iam::${ACCOUNT_ID}:oidc-provider/token.actions.githubusercontent.com\"},
    \"Action\":\"sts:AssumeRoleWithWebIdentity\",
    \"Condition\":{
      \"StringEquals\":{\"token.actions.githubusercontent.com:aud\":\"sts.amazonaws.com\"},
      \"StringLike\":{\"token.actions.githubusercontent.com:sub\":\"repo:influxdata/influxdb3_plugins:*\"}
    }
  }]
}" --profile $PROFILE

aws iam attach-role-policy --role-name "${BUCKET}-ci" \
  --policy-arn "arn:aws:iam::${ACCOUNT_ID}:policy/${BUCKET}-publish" --profile $PROFILE
```

## GitHub Secrets

| Secret | Purpose | How to create |
|--------|---------|---------------|
| `SDK_REPO_TOKEN` | Download CLI from private SDK repo | Fine-grained PAT → resource owner: `influxdata` → repo: `influxdb3-plugin-sdk` → permissions: Contents read-only |

```bash
gh secret set SDK_REPO_TOKEN --repo influxdata/influxdb3_plugins
```

## Workflow Configuration

In `.github/workflows/publish.yml`, update these values:

```yaml
env:
  S3_BUCKET: influxdb3-plugins-registry           # your bucket name
  AWS_REGION: us-east-2                            # your bucket region
  AWS_ROLE_ARN: arn:aws:iam::ACCOUNT:role/NAME     # your IAM role ARN
```

Pin the SDK version in `registry-guide/.sdk-version`.

## SDK Version Upgrades

1. Update `registry-guide/.sdk-version` to the new version
2. Verify the release exists: `gh release view v<version> --repo influxdata/influxdb3-plugin-sdk`
3. Test locally before pushing
4. Push — CI will fetch the new version

**Note:** The SDK CLI template was renamed from `new registry` to `new index` in v0.1.1. Use v0.1.1+.

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| CLI fetch 404 | Private repo, missing/expired `SDK_REPO_TOKEN` | Regenerate PAT, update secret |
| OIDC "not authorized to perform sts:AssumeRoleWithWebIdentity" | Trust policy doesn't match repo/branch | Check `sub` condition in IAM role trust |
| S3 upload "Access Denied" | IAM policy missing `s3:PutObject` | Check policy attached to CI role |
| S3 public access 403 | Block Public Access settings or missing bucket policy | Run the public-access-block and bucket-policy commands above |
| Bad index published | Script or SDK bug | Restore previous version via S3 versioning: `aws s3api list-object-versions --bucket $BUCKET --prefix index.json` then copy the prior version |
| Plugin not published after push | No version bump in `manifest.toml` | Bump version — the SDK skips already-published `(name, version)` pairs |
