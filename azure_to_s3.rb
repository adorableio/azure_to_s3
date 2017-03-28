# This relies on two environment variables to work:
# export AZURE_STORAGE_ACCOUNT=gtwww
# export AZURE_STORAGE_ACCESS_KEY=<azure storage access key>

require 'azure/storage'
require 'benchmark'

client = Azure::Storage::Client.create
blobs = client.blob_client

def count_objects(blobs, requests=1, count=0, time=0, marker=nil)
  objects, total, total_time, new_marker = nil

  result = Benchmark.measure do
    puts "Request: #{requests}"

    objects = blobs.list_blobs('images', marker: marker)
    puts "New Objects: #{objects.size}"

    total = objects.size + count
    puts "Total Objects: #{total}"

    new_marker = objects.continuation_token
    puts "Marker: #{new_marker}"
  end
  total_time = time + result.utime
  puts "Time: #{result.utime} (#{total_time})"

  return if objects.size == 0 || new_marker.nil?
  count_objects blobs, requests + 1, total, total_time, new_marker
end

count_objects(blobs)
