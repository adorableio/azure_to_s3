require_relative 'spec_helper'

describe AzureToS3::AzureBlobClient do
  describe '#fetch_blobs' do
    let(:client) { AzureToS3::AzureBlobClient.new('container', storage, blob_client) }
    let(:blob_client) { double('blob client', list_blobs: FakeResults.new) }
    let(:storage) { AzureToS3::InMemoryStorage.new }

    class FakeResults < Array
      attr_reader :continuation_token

      def initialize(opts={})
        super(opts[:results] || [])
        @continuation_token = opts[:continuation_token] || ''
      end
    end

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
end
