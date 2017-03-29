# export AZURE_STORAGE_ACCOUNT=gtwww
# export AZURE_STORAGE_ACCESS_KEY=<azure storage access key>
# export AWS_ACCESS_KEY_ID=<aws access key id>
# export AWS_SECRET_ACCESS_KEY=<aws secret access key>
# export AWS_REGION=<aws region ("us-east-1")

require 'azure/storage'
require 'aws-sdk'
require 'benchmark'

module AzureToS3
  class AzureBlobClient
    def initialize(container)
      @container = container
      @azure_client = Azure::Storage::Client.create
      @blob_client = @azure_client.blob_client
    end

    def blob_list
      @blob_client.list_blobs @container
    end

    def get_blob_content(blob_name)
      blob, content = @blob_client.get_blob @container, blob_name
      content
    end

    def fetch_blobs(storage)
      blob_list.each do |blob|
        props = blob.properties
        storage << {
          name: blob.name,
          md5_64: props.fetch(:content_md5),
          content_length: props.fetch(:content_length)
        }
      end
    end
  end

  class BlobValidator
    def initialize(client, blob)
      @client = client
      @blob = blob
    end

    def validate
      content = @client.get_blob_content @blob.fetch(:name)
      md5 = Digest::MD5.new.tap {|m| m.update(content) }.base64digest

      if md5 == @blob.fetch(:md5_64)
        @blob[:validated] = :md5
      elsif content.size == @blob.fetch(:content_length)
        @blob[:validated] = :length
      end

      yield(content) if @blob[:validated]
    end
  end

  class S3Client
    def initialize(bucket)
      @bucket = bucket
      @s3 = Aws::S3::Client.new
    end

    def upload_blob(blob, content)
      @s3.put_object bucket: @bucket, key: blob.fetch(:name), body: content
    end
  end
end

local_blobs = []
blob_client = AzureToS3::AzureBlobClient.new 'imagestos3'
blob_client.fetch_blobs local_blobs

s3_client = AzureToS3::S3Client.new 'azure-migration-test'

local_blobs.each do |blob|
  AzureToS3::BlobValidator.new(blob_client, blob).validate do |content|
    s3_client.upload_blob blob, content
  end

  if blob[:validated]
    puts "Successfully uploaded #{blob.fetch(:name)} (#{blob.fetch(:validated)})"
  else
    $stderr.puts "Blob failed checksum: #{blob.inspect}"
  end
end
