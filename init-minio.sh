#!/bin/bash
# Script to initialize MinIO bucket and policies

# Install Curl
apt-get update && apt-get install -y curl

# Wait for MinIO to be ready
echo "Waiting for MinIO to be ready..."
until curl -s http://ciad-minio:9001/minio/health/live; do
  echo "MinIO not ready yet, waiting..."
  sleep 5
done

echo "MinIO is ready, initializing..."

# Create the manifests bucket if it doesn't exist
mc alias set ciad-minio http://ciad-minio:9002 minioadmin minioadmin
mc mb --ignore-existing ciad-minio/manifests

# Set bucket policy to allow read/write access
cat > /tmp/bucket-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": ["*"]
      },
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": ["arn:aws:s3:::manifests/*"]
    }
  ]
}
EOF

mc policy set /tmp/bucket-policy.json ciad-minio/manifests

echo "MinIO initialization completed successfully"