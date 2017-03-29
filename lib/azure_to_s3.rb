require_relative 'azure_to_s3/marker_storage'
require_relative 'azure_to_s3/azure_blob_client'
require_relative 'azure_to_s3/s3_client'
require_relative 'azure_to_s3/blob_worker'
require_relative 'azure_to_s3/in_memory_blob_storage'
require_relative 'azure_to_s3/sequel_blob_storage'

marker_storage = AzureToS3::MarkerStorage.new File.expand_path(File.join(File.dirname(__FILE__), 'last_marker'))
blob_client = AzureToS3::AzureBlobClient.new 'imagestos3', marker_storage
s3_client = AzureToS3::S3Client.new 'azure-migration-test'

db = Sequel.postgres 'azure_to_s3'
blobs = AzureToS3::SequelBlobStorage.new db
blobs.setup_table

blob_client.fetch_blobs blobs
AzureToS3::BlobWorker.new(blobs, blob_client, s3_client).work
