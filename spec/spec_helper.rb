require_relative '../lib/azure_to_s3'

RSpec.configure do |config|
  config.before(:all) do
    $stderr = File.open(File::NULL, "w")
    $stdout = File.open(File::NULL, "w")
  end
end
