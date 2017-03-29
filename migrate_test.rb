# export AZURE_STORAGE_ACCOUNT=gtwww
# export AZURE_STORAGE_ACCESS_KEY=<azure storage access key>
# export AWS_ACCESS_KEY_ID=<aws access key id>
# export AWS_SECRET_ACCESS_KEY=<aws secret access key>
# export AWS_REGION=<aws region ("us-east-1")

require 'azure/storage'
require 'aws-sdk'
require 'benchmark'
require 'sequel'

module AzureToS3
  class MarkerStorage
    attr_reader :marker

    def initialize(file_name)
      @file_name = file_name
      @marker = File.read(@file_name) if File.exist?(@file_name)
    end

    def marker=(marker)
      if marker
        File.open(@file_name, 'w') {|f| f << marker }
      else
        FileUtils.rm_f(@file_name)
      end
      @marker = marker
    end
  end

  class AzureBlobClient
    def initialize(container, marker_storage, max_results=nil)
      @container = container
      @azure_client = Azure::Storage::Client.create
      @blob_client = @azure_client.blob_client
      @marker_storage = marker_storage
      @max_results = max_results
    end

    def fetch_blob_content(blob)
      _, content = @blob_client.get_blob @container, blob.fetch(:name)
      md5 = Digest::MD5.new.tap {|m| m.update(content) }.base64digest

      if md5 == blob.fetch(:md5_64)
        blob[:validated] = 'md5'
      elsif content.size == blob.fetch(:content_length)
        blob[:validated] = 'length'
      end

      yield(content) if blob[:validated]
    end

    def fetch_blobs(storage)
      each_blob do |blob|
        props = blob.properties
        storage << {
          name: blob.name,
          md5_64: props.fetch(:content_md5),
          content_length: props.fetch(:content_length)
        }
      end
    end

    private
    def each_blob(&block)
      @blob_client.list_blobs(@container, marker: @marker_storage.marker, max_results: @max_results).tap do |results|
        results.each &block

        marker = results.continuation_token
        marker = nil if marker.empty?
        @marker_storage.marker = marker
      end

      each_blob(&block) if @marker_storage.marker
    end
  end

  class S3Client
    def initialize(bucket)
      @bucket = bucket
      @s3 = Aws::S3::Client.new
    end

    def upload_blob(blob, content)
      @s3.put_object bucket: @bucket, key: blob.fetch(:name), body: content, content_md5: blob.fetch(:md5_64)
    end
  end

  class BlobWorker
    def initialize(storage, blob_client, s3_client)
      @storage = storage
      @blob_client = blob_client
      @s3_client = s3_client
    end

    def work
      @storage.each do |blob|
        @blob_client.fetch_blob_content(blob) do |content|
          @s3_client.upload_blob blob, content
          blob[:uploaded_to_s3] = true
          @storage.update blob
        end

        if blob[:validated]
          puts "Successfully uploaded #{blob.fetch(:name)} (#{blob.fetch(:validated)})"
        else
          $stderr.puts "Blob failed checksum: #{blob.inspect}"
        end
      end
    end
  end

  class InMemoryBlobStorage
    def initialize
      @blobs = []
    end

    def <<(blob)
      @blobs << blob
    end

    def each(&block)
      @blobs.each &block
    end

    def update(blob)
      # no-op... it's all in memory
    end
  end

  class SequelBlobStorage
    def initialize(db)
      @db = db
    end

    def <<(blob)
      @db[:blobs].insert(blob).tap {|id| blob[:id] = id }
    end

    def each(&block)
      while (@db.transaction {
        if record = @db['SELECT * FROM blobs WHERE uploaded_to_s3 IS FALSE ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1'].first
          block.call record
          record
        else
          puts "No records found, ending"
        end
      })
      end
    end

    def update(blob)
      @db[:blobs].where(id: blob[:id]).update(blob)
    end
  end
end

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
