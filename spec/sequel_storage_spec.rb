require_relative '../lib/azure_to_s3'

describe AzureToS3::SequelStorage do
  let(:db) { Sequel.sqlite }
  let(:storage) { AzureToS3::SequelStorage.new db }
  before { storage.setup_tables }

  describe '#<<(blob)' do
    context 'a new blob' do
      it 'adds the blob to the database' do
        expect { storage << { name: 'chicken' } }
          .to change { db[:blobs].count }.by(1)
      end

      it 'sets uploaded_to_s3 to false' do
        blob = { name: 'chicken' }
        storage << blob
        expect(blob[:uploaded_to_s3]).to be(false)
      end
    end

    context 'an existing blob (matching on name)' do
      let(:blob) { { name: 'chicken', file_md5_64: 'abc', content_length: 123 } }
      before { storage << blob }

      it 'does not create a new record' do
        expect { storage << blob }.to_not change { db[:blobs].count }
      end

      it 'does not set uploaded_to_s3 to true' do
        storage << blob
        expect(blob[:uploaded_to_s3]).to be(false)
        expect(db[:blobs][id: blob[:id]][:uploaded_to_s3]).to be(false)
      end

      context 'uploaded_to_s3=true' do
        before { db[:blobs].update(uploaded_to_s3: true) }

        it 'sets it to false when the file_md5_64 differs' do
          blob[:file_md5_64] = 'DEF'
          storage << blob
          expect(blob[:uploaded_to_s3]).to be(false)
          expect(db[:blobs][id: blob[:id]][:uploaded_to_s3]).to be(false)
        end

        it 'sets it to false when the content_length differs' do
          blob[:content_length] = 456
          storage << blob
          expect(blob[:uploaded_to_s3]).to be(false)
          expect(db[:blobs][id: blob[:id]][:uploaded_to_s3]).to be(false)
        end

        it 'does not set it to false when the existing file_md5_64 is nil' do
          db[:blobs].update(file_md5_64: nil)
          storage << blob
          expect(blob[:uploaded_to_s3]).to be(true)
          expect(db[:blobs][id: blob[:id]][:uploaded_to_s3]).to be(true)
        end
      end
    end
  end

  describe 'marker=(new_marker)' do
    it 'inserts a new marker' do
      expect { storage.marker = 'abc' }
        .to change { db[:marker].count }.by(1)
    end

    it 'updates an existing marker' do
      storage.marker = 'abc'
      expect { storage.marker = 'def' }
        .to_not change { db[:marker].count }
      expect(db[:marker].first[:marker]).to eq('def')
    end

    it 'raises an error if the marker is the same' do
      storage.marker = 'abc'
      expect { storage.marker = 'abc' }
        .to raise_error(/Cannot update with the same marker/)
    end

    it 'deletes an existing marker when the new marker is nil' do
      storage.marker = 'abc'
      expect { storage.marker = nil }
        .to change { db[:marker].count }.by(-1)
    end
  end
end