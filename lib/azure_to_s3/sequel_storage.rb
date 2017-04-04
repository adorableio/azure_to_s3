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
          String :azure_md5_64
          String :file_md5_64
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
        if existing[:uploaded_to_s3]
          blob[:uploaded_to_s3] = ((existing[:file_md5_64] && (blob[:file_md5_64] == existing[:file_md5_64])) ||
                                    (blob[:file_md5_64] && !existing[:file_md5_64])) &&
                                  (blob[:content_length] == existing[:content_length])
        end
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

    def marker=(new_marker)
      if new_marker
        if existing = @db[:marker].first
          raise "Cannot update with the same marker" if existing[:marker] == new_marker
          @db[:marker].update(marker: new_marker)
        else
          @db[:marker].insert(marker: new_marker)
        end
      else
        @db[:marker].delete
      end
    end

    def marker
      record = @db[:marker].first
      record[:marker] if record
    end

    def stats
      @db[
        'SELECT * FROM
          (SELECT COUNT(*) AS count_all FROM blobs) AS c1,
          (SELECT COUNT(*) AS count_uploaded FROM blobs WHERE uploaded_to_s3 IS TRUE) AS c2,
          (SELECT COUNT(*) AS validated_md5 FROM blobs WHERE validated=\'md5\') AS c3,
          (SELECT COUNT(*) AS validated_length FROM blobs WHERE validated=\'length\') AS c4
        '
      ].first.merge(marker: marker)
    end

    private
    def update(blob)
      @db[:blobs].where(id: blob[:id]).update(blob)
    end
  end
end
