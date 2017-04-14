require 'azure/storage'

module AzureToS3
  class AzureBlobClient
    def initialize(container, storage, blob_client, max_results=nil)
      @container = container
      @blob_client = blob_client
      @storage = storage
      @max_results = max_results
    end

    def fetch_blob_content(blob)
      begin
        _, content = @blob_client.get_blob @container, blob.fetch(:name)
      rescue Faraday::ConnectionFailed, Faraday::TimeoutError, Azure::Core::Http::HTTPError
        $stderr.puts "(fetch blob content) Connection failure, aborting this attempt..."
        return
      end

      md5 = Digest::MD5.new.tap {|m| m.update(content) }.base64digest
      blob[:file_md5_64] = md5

      azure_md5 = blob.fetch(:azure_md5_64).to_s # might be nil from azure?

      if !azure_md5.empty? && md5 == azure_md5
        blob[:validated] = 'md5'
      elsif !azure_md5.empty?
        blob[:validated] = nil
        blob[:validation_failed] = true
      elsif content.size == blob.fetch(:content_length)
        blob[:validated] = 'length'
      else
        blob[:validated] = nil
        blob[:validation_failed] = true
      end

      yield(content) if blob[:validated] && block_given?

      @storage << blob
    end

    def fetch_blobs
      each_blob do |blob|
        props = blob.properties
        @storage << {
          name: blob.name,
          azure_md5_64: props.fetch(:content_md5),
          content_length: props.fetch(:content_length)
        }
      end
    end

    private
    def each_blob(&block)
      puts "Listing blobs using marker: #{@storage.marker}"

      begin
        results = @blob_client.list_blobs(@container, marker: @storage.marker, max_results: @max_results)
        results.each &block
        marker = results.continuation_token
        marker = nil if marker.empty?
        @storage.marker = marker

        each_blob(&block) if marker
      rescue Faraday::ConnectionFailed, Faraday::TimeoutError, Azure::Core::Http::HTTPError
        $stderr.puts "(list blobs) Connection failed, sleeping for 3 seconds and retrying"
        sleep 3
        each_blob(&block)
      end
    end
  end
end
