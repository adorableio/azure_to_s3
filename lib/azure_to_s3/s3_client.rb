require 'aws-sdk'

module AzureToS3
  class S3Client
    def initialize(bucket)
      @bucket = bucket
      @s3 = Aws::S3::Client.new
    end

    def upload_blob(blob, content)
      @s3.put_object bucket: @bucket, key: blob.fetch(:name), body: content, content_md5: blob.fetch(:file_md5_64)
    end
  end
end
