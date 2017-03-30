require "rack"

module AzureToS3
  class StatsServer
    def initialize(storage)
      @storage = storage
    end

    def call(env)
      [200, {"Content-Type" => "text/plain"}, [@storage.stats.inspect]]
    end
  end
end
