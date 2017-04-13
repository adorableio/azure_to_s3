require_relative 'spec_helper'

describe AzureToS3::AzureBlobClient do
  describe '#fetch_blobs' do
    let(:client) { AzureToS3::AzureBlobClient.new('container', storage, blob_client) }
    let(:blob_client) { double('blob client', list_blobs: FakeResults.new) }
    let(:db) { Sequel.sqlite }
    let(:storage) { AzureToS3::SequelStorage.new db }
    before { storage.setup_tables }

    it 'lists blobs on the blob client' do
      expect(blob_client).to receive(:list_blobs).
        with('container', marker: nil, max_results: nil)

      client.fetch_blobs
    end

    it 'passes max_results to the blob client' do
      client = AzureToS3::AzureBlobClient.new('container', storage, blob_client, 2_000)
      expect(blob_client).to receive(:list_blobs).
        with('container', marker: nil, max_results: 2_000)

      client.fetch_blobs
    end

    it 'adds the returned blobs to storage' do
      allow(blob_client).to receive(:list_blobs).and_return(FakeResults.new results: [
        double('blob', name: 'my_blob', properties: { content_md5: 'the_md5', content_length: 'the_length' })
      ])
      expect(storage).to receive(:<<).with({
        name: 'my_blob',
        azure_md5_64: 'the_md5',
        content_length: 'the_length'
      })

      client.fetch_blobs
    end

    it 'saves the azure results marker to storage' do
      allow(blob_client).to receive(:list_blobs).and_return(FakeResults.new(continuation_token: 'the_token'),
                                                            FakeResults.new)
      expect(storage).to receive(:marker=).with('the_token')
      expect(storage).to receive(:marker=).with(nil)

      client.fetch_blobs
    end

    it 'sets the marker to nil when the marker is an empty string' do
      allow(blob_client).to receive(:list_blobs).and_return(FakeResults.new continuation_token: '')
      expect(storage).to receive(:marker=).with(nil)

      client.fetch_blobs
    end

    it 'calls the blob client multiple times if there is a continuation token' do
      expect(blob_client).to receive(:list_blobs)
        .with('container', marker: nil, max_results: nil)
        .and_return(FakeResults.new continuation_token: 'the_token', results: [
          double('blob1', name: 'my_blob_1', properties: { content_md5: 'the_md5_1', content_length: 'the_length_1' })
        ])
      expect(blob_client).to receive(:list_blobs)
        .with('container', marker: 'the_token', max_results: nil)
        .and_return(FakeResults.new results: [
          double('blob2', name: 'my_blob_2', properties: { content_md5: 'the_md5_2', content_length: 'the_length_2' })
        ])
      expect(storage).to receive(:<<).with({
        name: 'my_blob_1',
        azure_md5_64: 'the_md5_1',
        content_length: 'the_length_1'
      })
      expect(storage).to receive(:<<).with({
        name: 'my_blob_2',
        azure_md5_64: 'the_md5_2',
        content_length: 'the_length_2'
      })

      client.fetch_blobs
    end

    it 'retries when there is a connection error' do
      expect(blob_client).to receive(:list_blobs)
        .with('container', marker: nil, max_results: nil)
        .and_raise(Faraday::ConnectionFailed.new("connection failed"))
      expect(client).to receive(:sleep)
      expect(blob_client).to receive(:list_blobs)
        .with('container', marker: nil, max_results: nil)
        .and_return(FakeResults.new)

      client.fetch_blobs
    end
  end

  describe '#fetch_blob_content(blob)' do
    let(:client) { AzureToS3::AzureBlobClient.new('container', storage, blob_client) }
    let(:blob) { { name: 'the_blob', azure_md5_64: 'M6pHyAZoSEjBLvuY8pdXTw==', content_length: 11 } }
    let(:blob_client) { double('blob client', list_blobs: FakeResults.new, get_blob: [:ignore, 'md5_content']) }
    let(:storage) { AzureToS3::InMemoryStorage.new }

    it 'gets blob information from the blob client' do
      expect(blob_client).to receive(:get_blob).with('container', 'the_blob')
      client.fetch_blob_content blob
    end

    it 'sets file_md5_64 on the blob' do
      client.fetch_blob_content blob
      expect(blob[:file_md5_64]).to be
    end

    context 'content md5 matches azure md5' do
      it 'sets validated to md5' do
        client.fetch_blob_content blob
        expect(blob[:validated]).to eq('md5')
      end

      it 'yields the content' do
        content = nil
        client.fetch_blob_content(blob) {|c| content = c }
        expect(content).to eq('md5_content')
      end
    end

    context 'md5 does not match, content length does' do
      before { blob[:azure_md5_64] = nil }

      it 'sets validated to length' do
        client.fetch_blob_content blob
        expect(blob[:validated]).to eq('length')
      end

      it 'yields the content' do
        content = nil
        client.fetch_blob_content(blob) {|c| content = c }
        expect(content).to eq('md5_content')
      end
    end

    context 'neither md5 nor content length match' do
      before do
        blob[:azure_md5_64] = nil
        blob[:content_length] += 1
      end

      it 'sets validated to nil if neither md5 nor content length match' do
        client.fetch_blob_content blob
        expect(blob[:validated]).to be_nil
      end

      it 'does not yield the content' do
        content = nil
        client.fetch_blob_content(blob) {|c| content = c }
        expect(content).to be_nil
      end
    end

    it 'returns nil if the connection fails' do
      expect(blob_client).to receive(:get_blob)
        .and_raise(Faraday::ConnectionFailed.new('connection failed'))
      expect(client.fetch_blob_content(blob)).to be_nil
    end
  end
end
