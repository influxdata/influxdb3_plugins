#!/usr/bin/env bash
#
# publish.sh — dumb publish loop for the plugin registry.
#
# Iterates every immediate child directory under PUBLISH_ROOT, calling
# `influxdb3-plugin package` for each. The SDK CLI does all validation,
# packaging, hashing, and index manipulation. This script only orchestrates.
#
# Usage:
#   ./publish.sh <publish_root> <index_path> <output_dir>
#
# After a successful run:
#   - <output_dir>/index.json is the final derived index
#   - <output_dir>/artifacts/ contains all new .tar.gz artifacts
#
# Exit codes:
#   0 — all plugins processed (some may have been skipped as already-published)
#   1 — at least one plugin failed for a reason other than already-published

set -euo pipefail

PUBLISH_ROOT="${1:?Usage: publish.sh <publish_root> <index_path> <output_dir>}"
INDEX_PATH="${2:?Usage: publish.sh <publish_root> <index_path> <output_dir>}"
OUTPUT_DIR="${3:?Usage: publish.sh <publish_root> <index_path> <output_dir>}"

# Resolve to absolute paths.
PUBLISH_ROOT="$(cd "$PUBLISH_ROOT" && pwd)"
INDEX_PATH="$(cd "$(dirname "$INDEX_PATH")" && pwd)/$(basename "$INDEX_PATH")"
OUTPUT_DIR="$(mkdir -p "$OUTPUT_DIR" && cd "$OUTPUT_DIR" && pwd)"

# Working state: current index starts as the input index.
CURRENT_INDEX="$INDEX_PATH"
SCRATCH_DIR="$(mktemp -d)"
ARTIFACTS_DIR="$OUTPUT_DIR/artifacts"
mkdir -p "$ARTIFACTS_DIR"

trap 'rm -rf "$SCRATCH_DIR"' EXIT

PUBLISHED=0
SKIPPED=0
FAILED=0

for plugin_dir in "$PUBLISH_ROOT"/*/; do
  [ -d "$plugin_dir" ] || continue
  plugin_name="$(basename "$plugin_dir")"

  echo "--- Processing: $plugin_name"

  scratch="$SCRATCH_DIR/$plugin_name"
  rm -rf "$scratch"
  mkdir -p "$scratch"

  set +e
  output="$(influxdb3-plugin package "$plugin_dir" \
    --index "$CURRENT_INDEX" \
    --out "$scratch" \
    --output json 2>&1)"
  exit_code=$?
  set -e

  if [ $exit_code -eq 0 ]; then
    echo "  Published."
    # Promote derived index as current for next iteration.
    CURRENT_INDEX="$scratch/index.json"
    # Stage artifact(s) — there should be exactly one .tar.gz.
    mv "$scratch"/*.tar.gz "$ARTIFACTS_DIR/"
    PUBLISHED=$((PUBLISHED + 1))
  elif echo "$output" | grep -q '"package::already_published"'; then
    echo "  Skipped (already published)."
    SKIPPED=$((SKIPPED + 1))
  else
    echo "  FAILED:"
    echo "$output"
    FAILED=$((FAILED + 1))
  fi
done

# Copy final index to output dir.
cp "$CURRENT_INDEX" "$OUTPUT_DIR/index.json"

echo ""
echo "=== Summary ==="
echo "  Published: $PUBLISHED"
echo "  Skipped:   $SKIPPED"
echo "  Failed:    $FAILED"

if [ $FAILED -gt 0 ]; then
  echo "ERROR: $FAILED plugin(s) failed." >&2
  exit 1
fi
