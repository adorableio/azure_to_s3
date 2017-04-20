require_relative 'spec_helper'

describe 'fetching from azure' do
  let(:client) { AzureToS3::AzureBlobClient.new('container', storage, blob_client) }
  let(:blob_client) {
    double('blob client')
  }
  let(:blob_properties) { { content_md5: 'M6pHyAZoSEjBLvuY8pdXTw==', content_length: 11 } }
  let(:db) { Sequel.sqlite }
  let(:storage) { AzureToS3::SequelStorage.new db }

  before do
    allow(blob_client).to receive(:list_blobs)
      .and_return(
        FakeResults.new(results: [double('blob', name: 'my_blob', properties: blob_properties)])
      )
  end

  before { storage.setup_tables }
  before { client.fetch_blobs }

  it 'saves the blobs to storage' do
    expect(db[:blobs].first).to eq(
      id: 1,
      name: 'my_blob',
      azure_md5_64: 'M6pHyAZoSEjBLvuY8pdXTw==',
      file_md5_64: nil,
      content_length: 11,
      validated: nil,
      validation_failed: false,
      uploaded_to_s3: false,
      deleted: false
    )
  end

  context 'then uploading to S3' do
    let(:worker) { AzureToS3::BlobWorker.new(storage, client, s3_client) }
    let(:s3_client) { AzureToS3::S3Client.new 'bucket', s3_api }
    let(:s3_api) { double('s3 api', put_object: true) }

    before do
      allow(blob_client).to receive(:get_blob).with('container', 'my_blob')
        .and_return([:ignore, 'md5_content'])

      worker.work
    end

    context 'md5 matches' do
      it 'uploads the file to s3' do
        expect(s3_api).to have_received(:put_object).with(
          bucket: 'bucket',
          key: 'my_blob',
          body: 'md5_content',
          content_md5: 'M6pHyAZoSEjBLvuY8pdXTw=='
        )
      end

      it 'validates as md5 and saves the blob to the db' do
        expect(db[:blobs].first).to eq(
          id: 1,
          name: 'my_blob',
          azure_md5_64: 'M6pHyAZoSEjBLvuY8pdXTw==',
          file_md5_64: 'M6pHyAZoSEjBLvuY8pdXTw==',
          content_length: 11,
          validated: 'md5',
          validation_failed: false,
          uploaded_to_s3: true,
          deleted: false
        )
      end
    end

    context 'md5 is empty, content length matches' do
      let(:blob_properties) { super().merge(content_md5: '') }

      it 'uploads the file to s3' do
        expect(s3_api).to have_received(:put_object).with(
          bucket: 'bucket',
          key: 'my_blob',
          body: 'md5_content',
          content_md5: 'M6pHyAZoSEjBLvuY8pdXTw=='
        )
      end

      it 'validates as length and saves the blob to the db' do
        expect(db[:blobs].first).to eq(
          id: 1,
          name: 'my_blob',
          azure_md5_64: '',
          file_md5_64: 'M6pHyAZoSEjBLvuY8pdXTw==',
          content_length: 11,
          validated: 'length',
          validation_failed: false,
          uploaded_to_s3: true,
          deleted: false
        )
      end
    end

    context 'md5 is present but does not match' do
      let(:blob_properties) { super().merge(content_md5: 'no_match') }

      it 'does not upload the file to s3' do
        expect(s3_api).to_not have_received(:put_object)
      end

      it 'marks blob as having failed validation' do
        expect(db[:blobs].first).to eq(
          id: 1,
          name: 'my_blob',
          azure_md5_64: 'no_match',
          file_md5_64: 'M6pHyAZoSEjBLvuY8pdXTw==',
          content_length: 11,
          validated: nil,
          validation_failed: true,
          uploaded_to_s3: false,
          deleted: false
        )
      end
    end

    context 'empty md5, content length does not match' do
      let(:blob_properties) { super().merge content_md5: '', content_length: 12 }

      it 'does not upload the file to s3' do
        expect(s3_api).to_not have_received(:put_object)
      end

      it 'marks blob as having failed validation' do
        expect(db[:blobs].first).to eq(
          id: 1,
          name: 'my_blob',
          azure_md5_64: '',
          file_md5_64: 'M6pHyAZoSEjBLvuY8pdXTw==',
          content_length: 12,
          validated: nil,
          validation_failed: true,
          uploaded_to_s3: false,
          deleted: false
        )
      end
    end

    context 'and fetching again...' do
      before do
        allow(blob_client).to receive(:list_blobs)
          .and_return(
            FakeResults.new(results: [double('blob', name: 'my_blob', properties: blob_properties_2)])
          )

        client.fetch_blobs
      end

      let(:blob_properties_2) { blob_properties.clone }

      context 'details matches stored' do
        it 'leaves the record untouched' do
          expect(db[:blobs].first).to eq(
            id: 1,
            name: 'my_blob',
            azure_md5_64: 'M6pHyAZoSEjBLvuY8pdXTw==',
            file_md5_64: 'M6pHyAZoSEjBLvuY8pdXTw==',
            content_length: 11,
            validated: 'md5',
            validation_failed: false,
            uploaded_to_s3: true,
            deleted: false
          )
        end
      end

      context 'details do not match stored' do
        let(:blob_properties_2) {
          super().merge(content_md5: '', content_length: 12)
        }

        it 'sets details and resets file_md5_64, validated, validation_failed, and uploaded_to_s3' do
          expect(db[:blobs].first).to eq(
            id: 1,
            name: 'my_blob',
            azure_md5_64: '',
            file_md5_64: nil,
            content_length: 12,
            validated: nil,
            validation_failed: false,
            uploaded_to_s3: false,
            deleted: false
          )
        end
      end
    end
  end
end
