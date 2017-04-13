require_relative '../lib/azure_to_s3'

unless ENV['VERBOSE']
  RSpec.configure do |config|
    config.before(:all) do
      $stderr = File.open(File::NULL, "w")
      $stdout = File.open(File::NULL, "w")
    end
  end
end

class FakeResults < Array
  attr_reader :continuation_token

  def initialize(opts={})
    super(opts[:results] || [])
    @continuation_token = opts[:continuation_token] || ''
  end
end
