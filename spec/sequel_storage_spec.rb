require_relative '../lib/azure_to_s3'

describe AzureToS3::SequelStorage do
  describe '#<<(blob)' do
    let(:db) { Sequel.sqlite }
    let(:storage) { AzureToS3::SequelStorage.new db }
    before { storage.setup_tables }

    context 'a new blob' do
      it 'adds the blob to the database' do
        expect { storage << { name: 'chicken' } }
          .to change { db[:blobs].count }.by(1)
      end
    end

    context 'an existing blob (matching on name)' do
      let(:blob) { { name: 'chicken', md5_64: 'abc', content_length: 123 } }
      before { storage << blob }

      it 'does not create a new record' do
        expect { storage << blob }.to_not change { db[:blobs].count }
      end
    end
  end
end
