# Contributing Plugins

## Prerequisites

- `influxdb3-plugin` CLI ([releases](https://github.com/influxdata/influxdb3-plugin-sdk/releases)) — use the version in `.sdk-version`
- Push access to this repo

## Add a New Plugin

```bash
influxdb3-plugin new <trigger_type> registry-guide/plugins/<name> --name <name>
```

Trigger types: `process_writes`, `process_scheduled_call`, `process_request`.

This creates `manifest.toml`, `__init__.py`, and `README.md`. Edit them to implement your plugin.

## Update an Existing Plugin

Edit the code, then **bump the version in `manifest.toml`**. Without a version bump, the change will not be published — the registry is versioned and immutable.

## Test Locally

```bash
# Fetch the current live index
curl -sS "https://influxdb3-plugins-registry.s3.us-east-2.amazonaws.com/index.json" > /tmp/index.json

# Run the publish loop
registry-guide/scripts/publish.sh registry-guide/plugins /tmp/index.json /tmp/output

# Inspect results
cat /tmp/output/index.json | python3 -m json.tool
ls /tmp/output/artifacts/
```

Your new or bumped plugin should show as "Published." Existing versions show as "Skipped (already published)."

## Push

Commit your changes and push to the demo branch. CI runs automatically and publishes new versions to S3.

Verify after CI completes:

```bash
curl -sS "https://influxdb3-plugins-registry.s3.us-east-2.amazonaws.com/index.json" | python3 -m json.tool
```

## Key Rules

- **Version bump = publish.** No bump, no publish.
- **Versions are immutable.** Once published, a `(name, version)` can never be overwritten.
- **Plugin names** must start with a letter, contain only `[a-zA-Z0-9_-]`, and be 1-64 characters.
- **Every directory under `plugins/`** must be a valid plugin (with `manifest.toml` and `__init__.py`). Don't put non-plugin files there.
