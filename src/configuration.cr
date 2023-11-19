module Conveyor
  private CONFIG = Configuration.new

  def self.configure
    yield CONFIG
    nil
  end

  class Configuration
    property redis : Redis::Client { Redis::Client.from_env "REDIS_URL" }
    property concurrency : Int32 { ENV.fetch("CONVEYOR_CONCURRENCY", "10").to_i }
    property queues : Array(String) { ENV.fetch("CONVEYOR_QUEUES", "default").split(',').map(&.strip) }
  end
end
