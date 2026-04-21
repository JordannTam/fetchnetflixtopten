#!/usr/bin/env bash
set -euo pipefail

FUNCTION_NAME="${FUNCTION_NAME:-netflix-top10-scraper}"
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
PACKAGE_DIR="$PROJECT_DIR/package"
ZIP_FILE="$PROJECT_DIR/lambda.zip"

# --- Build ---

echo "Cleaning previous build..."
rm -rf "$PACKAGE_DIR" "$ZIP_FILE"

echo "Installing dependencies..."
pip3 install -r "$PROJECT_DIR/requirements.txt" -t "$PACKAGE_DIR" --quiet

echo "Creating lambda.zip..."
cd "$PACKAGE_DIR" && zip -r "$ZIP_FILE" . --quiet
cd "$PROJECT_DIR" && zip "$ZIP_FILE" -r src/ --quiet

ZIP_SIZE=$(du -h "$ZIP_FILE" | cut -f1)
echo "Build complete: lambda.zip ($ZIP_SIZE)"

# --- Deploy ---

if ! command -v aws &>/dev/null; then
  echo "aws CLI not found — skipping deploy. Install it to enable one-step deploy."
  exit 0
fi

echo "Deploying to Lambda function: $FUNCTION_NAME..."
aws lambda update-function-code \
  --function-name "$FUNCTION_NAME" \
  --zip-file "fileb://$ZIP_FILE" \
  --output json \
  | python3 -c "
import json, sys
r = json.load(sys.stdin)
print(f\"  Runtime:      {r.get('Runtime')}\")
print(f\"  Code size:    {r.get('CodeSize', 0) // 1024} KB\")
print(f\"  Last modified: {r.get('LastModified')}\")
print(f\"  Status:       {r.get('LastUpdateStatus')}\")
"

echo "Done. Test it with:"
echo "  aws lambda invoke --function-name $FUNCTION_NAME --payload '{}' --cli-binary-format raw-in-base64-out response.json && cat response.json"
