# export AZURE_STORAGE_ACCOUNT=gtwww
# export AZURE_STORAGE_ACCESS_KEY=<azure storage access key>
# export AWS_ACCESS_KEY_ID=<aws access key id>
# export AWS_SECRET_ACCESS_KEY=<aws secret access key>
# export AWS_REGION=<aws region ("us-east-1")

require 'azure/storage'
require 'aws-sdk'
require 'benchmark'

$azure_container = 'imagestos3'
$s3_bucket = 'azure-migration-test'

# List blobs in container
azure_client = Azure::Storage::Client.create
blob_client = azure_client.blob_client
blob_list = blob_client.list_blobs $azure_container

# Store blob information in a local database (name, md5, content length)
local_blobs = []

blob_list.each do |blob|
  props = blob.properties
  local_blobs << {
    name: blob.name,
    md5_64: props.fetch(:content_md5),
    content_length: props.fetch(:content_length)
  }
end

s3 = Aws::S3::Client.new

# Fetch blob content
# Upload blob to S3, verify md5 and content length
local_blobs.each do |blob|
  blob_info, content = blob_client.get_blob $azure_container, blob.fetch(:name)
  md5 = Digest::MD5.new.tap {|m| m.update(content) }.base64digest

  if md5 == blob.fetch(:md5_64)
    blob[:validated] = :md5
  elsif content.size == blob.fetch(:content_length)
    blob[:validated] = :length
  end

  if blob[:validated]
    s3.put_object bucket: $s3_bucket, key: blob.fetch(:name), body: content
    puts "Successfully uploaded #{blob.fetch(:name)} (#{blob.fetch(:validated)})"
  else
    $stderr.puts "Blob failed checksum: #{blob.inspect}"
  end
end
