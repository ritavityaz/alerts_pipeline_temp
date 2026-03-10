#!/bin/bash
set -e

FUNCTION_NAME="alerts-pipeline"
REGION="us-east-2"
BUCKET="alerts-dashboard-data"
S3_KEY="lambda/package.zip"
ROLE_ARN="$1"

if [ -z "$ROLE_ARN" ]; then
  echo "Usage: ./deploy.sh <lambda-role-arn>"
  echo "  e.g. ./deploy.sh arn:aws:iam::430576513331:role/alerts-pipeline-role"
  exit 1
fi

echo "==> Building deployment package..."
rm -rf build package.zip
mkdir build

# Install dependencies into build/
pip install --target build/ -r requirements.txt --platform manylinux2014_x86_64 --only-binary=:all: -q

# Copy function code + data
cp lambda_function.py build/
cp cities.csv build/

# Create zip
cd build
zip -r ../package.zip . -q
cd ..

SIZE=$(du -h package.zip | cut -f1)
echo "==> Package size: $SIZE"

# Upload to S3 (bypasses 50MB direct upload limit)
echo "==> Uploading to s3://$BUCKET/$S3_KEY ..."
aws s3 cp package.zip "s3://$BUCKET/$S3_KEY" --region $REGION

# Check if function exists
if aws lambda get-function --function-name $FUNCTION_NAME --region $REGION 2>/dev/null; then
  echo "==> Updating existing function..."
  aws lambda update-function-code \
    --function-name $FUNCTION_NAME \
    --s3-bucket $BUCKET \
    --s3-key $S3_KEY \
    --region $REGION
else
  echo "==> Creating new function..."
  aws lambda create-function \
    --function-name $FUNCTION_NAME \
    --runtime python3.12 \
    --handler lambda_function.lambda_handler \
    --role $ROLE_ARN \
    --code S3Bucket=$BUCKET,S3Key=$S3_KEY \
    --timeout 900 \
    --memory-size 512 \
    --region $REGION
fi

echo "==> Done! Function: $FUNCTION_NAME"
echo ""
echo "Next steps:"
echo "  1. Add EventBridge rule: rate(1 hour) -> $FUNCTION_NAME"
echo "  2. Test: aws lambda invoke --function-name $FUNCTION_NAME --region $REGION /dev/stdout"

# Cleanup
rm -rf build
