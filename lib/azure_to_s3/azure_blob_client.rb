require 'azure/storage'

module AzureToS3
  class AzureBlobClient
    def initialize(container, storage, max_results=nil)
      @container = container
      @azure_client = Azure::Storage::Client.create
      @blob_client = @azure_client.blob_client
      @storage = storage
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

    def fetch_blobs
      each_blob do |blob|
        props = blob.properties
        @storage << {
          name: blob.name,
          md5_64: props.fetch(:content_md5),
          content_length: props.fetch(:content_length)
        }
      end
    end

    private
    def each_blob(&block)
      @blob_client.list_blobs(@container, marker: @storage.marker, max_results: @max_results).tap do |results|
        results.each &block

        marker = results.continuation_token
        marker = nil if marker.empty?
        @storage.marker = marker
      end

      each_blob(&block) if @storage.marker
    end
  end
end
