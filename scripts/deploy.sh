#!/usr/bin/env bash
set -euo pipefail

# Deploy the UC Data Quality Explorer via Databricks Asset Bundles.
#
# Usage:
#   ./scripts/deploy.sh [--target TARGET] [--profile PROFILE]
#
# Examples:
#   ./scripts/deploy.sh                          # deploy dev target
#   ./scripts/deploy.sh --target prod --profile my-ws

TARGET="dev"
PROFILE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target)  TARGET="$2"; shift 2 ;;
    --profile) PROFILE="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

PROFILE_FLAG=""
if [[ -n "$PROFILE" ]]; then
  PROFILE_FLAG="--profile $PROFILE"
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

echo "=== Validating bundle ==="
databricks bundle validate --target "$TARGET" $PROFILE_FLAG

echo ""
echo "=== Deploying bundle (target: $TARGET) ==="
databricks bundle deploy --target "$TARGET" $PROFILE_FLAG

USER_EMAIL=$(databricks auth env $PROFILE_FLAG 2>/dev/null \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('env',{}).get('DATABRICKS_USER',''))" 2>/dev/null || echo "")
BUNDLE_PATH="/Workspace/Users/${USER_EMAIL}/.bundle/dfe-data-quality/${TARGET}/files"

echo ""
echo "=== Deploying app source from: $BUNDLE_PATH ==="
databricks apps deploy dfe-data-quality --source-code-path "$BUNDLE_PATH" $PROFILE_FLAG

echo ""
echo "=== Deployment complete ==="
databricks apps get dfe-data-quality $PROFILE_FLAG 2>/dev/null \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(f'App URL: {d.get(\"url\",\"pending\")}')" 2>/dev/null \
  || echo "(Check app status with: databricks apps get dfe-data-quality)"
