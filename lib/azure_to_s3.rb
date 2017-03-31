require_relative 'azure_to_s3/azure_blob_client'
require_relative 'azure_to_s3/s3_client'
require_relative 'azure_to_s3/blob_worker'
require_relative 'azure_to_s3/in_memory_storage'
require_relative 'azure_to_s3/sequel_storage'
require_relative 'azure_to_s3/stats_server'

module AzureToS3
  def self.setup_all
    setup_storage
    @blob_client = AzureBlobClient.new ENV.fetch('AZURE_TO_S3_CONTAINER'), @storage
    @s3_client = S3Client.new ENV.fetch('AZURE_TO_S3_BUCKET')
  end

  def self.setup_storage(adapter=ENV.fetch('ADAPTER', 'memory').to_sym)
    @adapter = adapter

    case @adapter
    when :memory
      @storage = InMemoryStorage.new
    when :postgres
      postgres_connection = ENV.fetch('AZURE_TO_S3_POSTGRES')
      db = if postgres_connection.include?('postgres://')
        Sequel.connect postgres_connection
      else
        Sequel.postgres postgres_connection
      end
      @storage = SequelStorage.new db
      @storage.setup_tables
    else
      raise "Unknown adapter: #{adapter}"
    end
  end

  def self.fetch
    setup_all
    @blob_client.fetch_blobs
  end

  def self.put
    setup_all
    fetch if @adapter == :memory
    BlobWorker.new(@storage, @blob_client, @s3_client).work
  end

  def self.stats_server
    setup_storage
    StatsServer.new(@storage)
  end
end
