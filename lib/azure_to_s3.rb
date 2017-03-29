require_relative 'azure_to_s3/azure_blob_client'
require_relative 'azure_to_s3/s3_client'
require_relative 'azure_to_s3/blob_worker'
require_relative 'azure_to_s3/in_memory_storage'
require_relative 'azure_to_s3/sequel_storage'

module AzureToS3
  def self.setup(adapter=ENV.fetch('ADAPTER', 'memory').to_sym)
    @adapter = adapter

    case @adapter
    when :memory
      @storage = InMemoryStorage.new
    when :postgres
      db = Sequel.postgres 'azure_to_s3'
      @storage = SequelStorage.new db
      @storage.setup_tables
    else
      raise "Unknown adapter: #{adapter}"
    end

    @blob_client = AzureBlobClient.new 'imagestos3', @storage
    @s3_client = S3Client.new 'azure-migration-test'
  end

  def self.fetch
    setup
    @blob_client.fetch_blobs
  end

  def self.put
    setup
    fetch if @adapter == :memory
    BlobWorker.new(@storage, @blob_client, @s3_client).work
  end
end
