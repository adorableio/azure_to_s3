require 'sequel'

module AzureToS3
  class SequelStorage
    def initialize(db)
      @db = db
    end

    def setup_tables
      unless @db.table_exists?(:blobs)
        @db.create_table :blobs do
          primary_key :id
          String :name, null: false, unique: true
          String :md5_64
          Integer :content_length
          String :validated
          Boolean :uploaded_to_s3, default: false, null: false

          index :validated
          index :uploaded_to_s3
        end
      end

      unless @db.table_exists?(:marker)
        @db.create_table :marker do
          String :marker, null: false
        end
      end
    end

    def <<(blob)
      if existing = @db[:blobs].where(name: blob[:name]).first
        blob[:id] = existing[:id]
        blob[:uploaded_to_s3] = (blob[:md5_64] == existing[:md5_64]) && (blob[:content_length] == existing[:content_length])
        update(blob)
      else
        blob[:uploaded_to_s3] = false
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

    def marker=(marker)
      if marker
        if existing = @db[:marker].first
          existing.update(marker: marker)
        else
          @db[:marker].insert(marker: marker)
        end
      else
        @db[:marker].delete
      end
    end

    def marker
      record = @db[:marker].first
      record[:marker] if record
    end
  end
end
