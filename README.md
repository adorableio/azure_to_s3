# Azure to S3

## Export Azure blob storage to Amazon S3

Downloading information from Azure relies on the following environment variables:

```bash
export AZURE_STORAGE_ACCOUNT=<azure storage account>
export AZURE_STORAGE_ACCESS_KEY=<azure storage access key>
```

Uploading to S3 relies on the following environment variables:

```bash
export AWS_ACCESS_KEY_ID=<aws access key id>
export AWS_SECRET_ACCESS_KEY=<aws secret access key>
export AWS_REGION=<aws region ("us-east-1")
```
