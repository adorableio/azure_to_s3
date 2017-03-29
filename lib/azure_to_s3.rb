# export AZURE_STORAGE_ACCOUNT=gtwww
# export AZURE_STORAGE_ACCESS_KEY=<azure storage access key>
# export AWS_ACCESS_KEY_ID=<aws access key id>
# export AWS_SECRET_ACCESS_KEY=<aws secret access key>
# export AWS_REGION=<aws region ("us-east-1")
$:.unshift File.dirname(__FILE__)
require 'azure_to_s3/marker_storage'
require 'azure_to_s3/azure_blob_client'
require 'azure_to_s3/s3_client'
require 'azure_to_s3/blob_worker'
require 'azure_to_s3/in_memory_blob_storage'
require 'azure_to_s3/sequel_blob_storage'

marker_storage = AzureToS3::MarkerStorage.new File.expand_path(File.join(File.dirname(__FILE__), 'last_marker'))
blob_client = AzureToS3::AzureBlobClient.new 'imagestos3', marker_storage
s3_client = AzureToS3::S3Client.new 'azure-migration-test'

db = Sequel.postgres 'azure_to_s3'

unless db.table_exists?(:blobs)
  db.create_table :blobs do
    primary_key :id
    String :name
    String :md5_64
    Integer :content_length
    String :validated
    Boolean :uploaded_to_s3, default: false, null: false
  end
end

blobs = AzureToS3::SequelBlobStorage.new db

blob_client.fetch_blobs blobs
AzureToS3::BlobWorker.new(blobs, blob_client, s3_client).work
