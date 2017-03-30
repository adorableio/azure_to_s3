module AzureToS3
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
          @storage << blob
        end

        if blob[:validated]
          puts "Successfully uploaded #{blob.fetch(:name)} (#{blob.fetch(:validated)})"
        else
          $stderr.puts "Blob failed checksum: #{blob.inspect}"
        end
      end
    end
  end
end
