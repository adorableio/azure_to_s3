require "rack"

module AzureToS3
  class StatsServer
    def initialize(storage)
      @storage = storage
    end

    def call(env)
      [200, {"Content-Type" => "text/plain"}, [stats.inspect]]
    end

    def stats
      @storage.stats.merge(
        uploaders: `ps ax | grep put_to_s3 | grep ruby2.3 | grep -v grep | wc -l`.strip.to_i,
        fetching: `ps ax | grep fetch_from_azure | grep ruby | grep -v grep | wc -l`.strip.to_i > 0
      )
    end
  end
end
