require 'sequel'

module AzureToS3
  class SequelBlobStorage
    def initialize(db)
      @db = db
    end

    def <<(blob)
      if existing = @db[:blobs].where(name: blob[:name]).first
        blob[:id] = existing[:id]
        update(blob)
      else
        @db[:blobs].insert(blob).tap {|id| blob[:id] = id }
      end
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
