module AzureToS3
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
end
