module Conveyor
  CONFIG = Configuration.new

  def self.configure(&)
    yield CONFIG
    nil
  end

  class Configuration
    # The client that `Conveyor::Belt`s will use to connect to Redis, defaults to
    # `Redis::Client.from_env("REDIS_URL")`. Rather than simply assigning a URL
    # for Redis to connect to, you assign the Redis instance itself.
    property redis : Redis::Client do
      Redis::Client.from_env "REDIS_URL"
    end

    # The number of `Conveyor::Belt` instances the orchestrator will create,
    # defaulting to the `CONVEYOR_CONCURRENCY` environment variable or `10` if
    # that isn't set.
    property concurrency : Int32 do
      ENV.fetch("CONVEYOR_CONCURRENCY", "10").to_i
    end

    # The list of queues that `Conveyor::Belt` instances will fetch jobs from,
    # defaulting to the comma-separated list of queue names in the
    # `CONVEYOR_QUEUES` environment variable or `["default"]` if it is not set.
    #
    # ```
    # bin/conveyor # queues will be `["default"]` if unset in code
    # CONVEYOR_QUEUES=low,medium,high bin/conveyor # queues will be ["low", "medium", "high"] if unset in code
    # ```
    #
    # NOTE: If you set this in code, it will override the `CONVEYOR_QUEUES`
    # environment variable.
    property queues : Array(String) do
      ENV.fetch("CONVEYOR_QUEUES", "default").split(',').map(&.strip)
    end

    # Check for orphaned jobs on this interval, defaulting to 10 minutes and
    # also configurable via the `CONVEYOR_ORPHAN_CHECK_DURATION_MIN`
    # environment variable.
    #
    # Orphaned jobs are jobs that were fetched from a queue but not closed out
    # properly. This can happen in several circumstances. A few common examples:
    # - a job consumes too much memory and is sent a [`Signal::KILL`](https://crystal-lang.org/api/1.15.0/Signal.html#KILL)
    # - during deployment, a job takes longer to complete than the deployment
    #   orchestrator allows for shutdown
    property orphan_check_interval : Time::Span do
      ENV.fetch("CONVEYOR_ORPHAN_CHECK_DURATION_MIN", "10").to_f.minutes
    end

    # The maximum number of times a job will attempt to execute, defaults to the
    # `CONVEYOR_JOB_MAX_ATTEMPTS` environment variable or `25` if it isn't set.
    property max_attempts : Int32 do
      ENV.fetch("CONVEYOR_JOB_MAX_ATTEMPTS", "25").to_i32
    end
  end
end
