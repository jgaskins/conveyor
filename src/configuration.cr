module Conveyor
  private CONFIG = Configuration.new

  def self.configure
    yield CONFIG
    nil
  end

  class Configuration
    property redis : Redis::Client do
      Redis::Client.from_env "REDIS_URL"
    end
    property concurrency : Int32 do
      ENV.fetch("CONVEYOR_CONCURRENCY", "10").to_i
    end
    property queues : Array(String) do
      ENV.fetch("CONVEYOR_QUEUES", "default").split(',').map(&.strip)
    end
    property orphan_check_interval : Time::Span do
      ENV.fetch("CONVEYOR_ORPHAN_CHECK_DURATION_MIN", "10").to_f.minutes
    end
    property max_attempts : Int32 do
      ENV.fetch("CONVEYOR_JOB_MAX_ATTEMPTS", "25").to_i32
    end
  end
end
