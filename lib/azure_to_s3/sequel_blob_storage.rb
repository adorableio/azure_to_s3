require 'sequel'

module AzureToS3
  class SequelBlobStorage
    def initialize(db)
      @db = db
    end

    def setup_table
      unless @db.table_exists?(:blobs)
        @db.create_table :blobs do
          primary_key :id
          String :name
          String :md5_64
          Integer :content_length
          String :validated
          Boolean :uploaded_to_s3, default: false, null: false
        end
      end
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
