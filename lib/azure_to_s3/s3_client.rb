require 'aws-sdk'

module AzureToS3
  class S3Client
    def initialize(bucket, s3=Aws::S3::Client.new)
      @bucket = bucket
      @s3 = s3
    end

    def upload_blob(blob, content)
      @s3.put_object bucket: @bucket, key: blob.fetch(:name), body: content, content_md5: blob.fetch(:file_md5_64)
      blob[:uploaded_to_s3] = true
    end
  end
end
