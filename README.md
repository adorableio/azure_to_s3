# Azure to S3

## Export Azure blob storage to Amazon S3

Set the following environment variables:

```bash
export AZURE_STORAGE_ACCOUNT=<azure storage account>
export AZURE_STORAGE_ACCESS_KEY=<azure storage access key>
export AZURE_TO_S3_CONTAINER=<azure container name>

export AWS_ACCESS_KEY_ID=<aws access key id>
export AWS_SECRET_ACCESS_KEY=<aws secret access key>
export AWS_REGION=<aws region ("us-east-1")>
export AZURE_TO_S3_BUCKET=<aws s3 bucket>

# if using postgres local storage...
export AZURE_TO_S3_POSTGRES=<postgres db name or connection string>
```

## Commands

### In-memory storage

```bash
./bin/put_to_s3
```

### Postgres local storage

```bash
ADAPTER=postgres ./bin/fetch_from_azure
ADAPTER=postgres ./bin/put_to_s3
```

## TODO

* handle case where we re-fetch from azure
