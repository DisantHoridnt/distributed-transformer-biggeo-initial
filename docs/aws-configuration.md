# AWS Configuration

## S3 Setup

### Bucket Configuration

1. Create or use an existing S3 bucket
2. Configure bucket policy to allow necessary operations:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "PublicReadGetObject",
            "Effect": "Allow",
            "Principal": "*",
            "Action": [
                "s3:GetObject",
                "s3:PutObject"
            ],
            "Resource": "arn:aws:s3:::your-bucket-name/*"
        }
    ]
}
```

### IAM Configuration

1. Create an IAM user with S3 access
2. Attach the following policy:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::your-bucket-name",
                "arn:aws:s3:::your-bucket-name/*"
            ]
        }
    ]
}
```

## Environment Variables

Required environment variables:
```env
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=us-east-1
```

## S3 Endpoint Configuration

The application is configured to use the AWS S3 endpoint:
```rust
.with_endpoint("https://s3.us-east-1.amazonaws.com")
```

## Troubleshooting

Common issues and solutions:

1. 403 Forbidden Error
   - Check IAM permissions
   - Verify bucket policy
   - Ensure environment variables are set correctly

2. Connection Issues
   - Verify endpoint URL
   - Check region configuration
   - Ensure network connectivity
