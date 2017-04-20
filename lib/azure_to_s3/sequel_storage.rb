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
          Boolean :validation_failed, default: false, null: false
          Boolean :uploaded_to_s3, default: false, null: false
          Boolean :deleted, default: false, null: false

          index :validated
          index :validation_failed
          index :uploaded_to_s3
          index [:uploaded_to_s3, :validation_failed]
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
        blob[:deleted] = false

        if existing[:uploaded_to_s3] &&
            (existing.fetch(:azure_md5_64) != blob.fetch(:azure_md5_64) ||
             existing.fetch(:content_length) != blob.fetch(:content_length))
          blob[:uploaded_to_s3] = false
          blob[:validated] = nil
          blob[:file_md5_64] = nil
        end

        update(blob)
      else
        blob[:uploaded_to_s3] = false
        @db[:blobs].insert(blob).tap {|id| blob[:id] = id }
      end
    end

    def delete(blob)
      @db[:blobs].where(id: blob.fetch(:id)).update(deleted: true)
    end

    def file_md5_64_matches?(existing, blob)
      (blob[:file_md5_64] && !existing[:file_md5_64]) ||
        (existing[:file_md5_64] && (blob[:file_md5_64] == existing[:file_md5_64]))
    end

    def content_length_matches?(existing, blob)
      blob[:content_length] == existing[:content_length]
    end

    def each(&block)
      while (@db.transaction {
        if record = @db["SELECT * FROM blobs WHERE NOT uploaded_to_s3 AND NOT validation_failed AND NOT deleted #{'FOR UPDATE SKIP LOCKED' if postgres?} LIMIT 1"].first
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
          (SELECT COUNT(*) AS validated_length FROM blobs WHERE validated=\'length\') AS c4,
          (SELECT COUNT(*) AS validation_failed FROM blobs WHERE validation_failed) AS c5
        '
      ].first.merge(marker: marker)
    end

    private
    def update(blob)
      @db[:blobs].where(id: blob[:id]).update(blob)
    end

    def postgres?
      @db.adapter_scheme == :postgres
    end
  end
end
