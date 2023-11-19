require "./belt"
require "./configuration"

module Conveyor
  class Orchestrator
    @config : Configuration
    @belts : Array(Belt)
    @running = false

    def initialize(configuration @config, @log = Log.for("conveyor"))
      @belts = Array.new(config.concurrency) do
        Belt.new(config.redis, config.queues, log: @log)
      end
    end

    def start
      @running = true
      concurrency = @config.concurrency
      channel = Channel(Nil).new(concurrency)
      @belts.each do |belt|
        spawn { belt.start { channel.send nil } }
      end
      spawn stats
      spawn check_for_orphans
      spawn check_for_scheduled

      while @running
        sleep 100.milliseconds
      end

      @belts.each_with_index do |belt, index|
        @log.info &.emit "Waiting for belt to stop", belt_id: belt.id, index: index
        channel.receive
        @log.info &.emit "Stopped belt", belt_id: belt.id, index: index
      end

      @log.info { "All belts stopped. Shutting down." }
    end

    def stop
      @running = false
      @belts.each(&.stop)
    end

    @on_error : (::Exception -> Nil) = ->(ex : ::Exception) {}

    def on_error(&@on_error : ::Exception -> Nil) : self
      @belts.each(&.on_error(&block))
      self
    end

    def stats
      while @running
        start = Time.monotonic
        sleep 10.seconds
        finish = Time.monotonic
        working = @belts.count(&.state.working?)
        jobs_per_second = @belts.sum(&.jobs_per_second)
        @log.notice &.emit "stats",
          working: working,
          jobs_per_second: jobs_per_second
      end
    end

    # Orphan jobs are jobs that were started but were not finished by the time
    # the belt was shut down. This method will periodically scan for those jobs
    # and re-enqueue them.
    private def check_for_orphans
      interval = 10.minutes

      while @running
        sleep interval

        @log.info { "Checking for orphans" }

        begin
          scan_for_orphans(
            belt_presence_duration: interval * 2,
            orphan_check_lock_duration: interval - 1.millisecond,
          )
        rescue ex
          @on_error.call ex
        end

        @log.info { "Orphan scan complete" }
      end
    end

    # :nodoc:
    def scan_for_orphans(belt_presence_duration : Time::Span, orphan_check_lock_duration : Time::Span)
      redis = @config.redis

      # Ensure we refresh the existence keys for all of the belts
      redis.pipeline do |pipe|
        @belts.each do |belt|
          pipe.set "conveyor:belt:#{belt.id}", "", ex: belt_presence_duration
        end
      end

      if redis.set("conveyor:lock:orphan-check", "", nx: true, ex: orphan_check_lock_duration)
        belt_id_cache = Hash(String, Bool).new do |cache, key|
          cache[key] = redis.exists("conveyor:belt:#{key}") == 0
        end
        now = Time.utc.to_unix_ms

        # Begin crawling through Redis for Conveyor jobs
        redis.scan_each match: "conveyor:job:*" do |key|
          # Only jobs that have been scheduled or picked up by a belt count as
          # orphans, and we also need to know which queue to enqueue it under
          # if it is indeed an orphan, so we select only those fields.
          if raw_job_data = redis.hmget(key, "queue", "pending", "belt_id").as?(Array)
            queue, pending, belt_id = raw_job_data

            # Job ids are "conveyor:job:#{id}"
            id = key.lchop "conveyor:job:"

            # If it was set as pending and it has a belt id that no longer
            # matches a running belt, then the job is orphaned and needs to be
            # requeued in its original queue.
            if pending && belt_id && belt_id_cache[belt_id] == 0
              redis.rpush "conveyor:queue:#{queue}", id
            end
          end
        end
      end
    end

    def check_for_scheduled
      while @running
        sleep 1.second

        begin
          enqueue_scheduled_jobs
        rescue ex
          @on_error.call ex
        end
      end
    end

    def enqueue_scheduled_jobs
      redis = @config.redis

      ids = redis
        .zrange("conveyor:scheduled", "-inf"..Time.utc.to_unix_ms, by: :score)
        .as(Array)
        .map(&.as(String))

      ids_and_queues = redis.pipeline do |pipe|
        ids.each do |id|
          pipe.hmget("conveyor:job:#{id}", "id", "queue")
        end
      end

      if ids.any?
        redis.multi do |txn|
          ids_and_queues.each do |results|
            id, queue = results.as(Array)
            if id && queue
              txn.rpush "conveyor:queue:#{queue}", id.as(String)
            end
          end

          txn.zrem "conveyor:scheduled", ids
        end
      end
    end
  end
end

module Redis
  module Commands::SortedSet
    def zrange(
      key : String,
      range : Range(Value, Value),
      *,
      by sort_type : SortType? = nil,
      rev reverse : Bool? = nil,
      limit : {Int64, Int64}? = nil,
      withscores : Bool? = nil
    )
      command = Array(String).new(initial_capacity: 10)
      command << "zrange" << key << range.begin.to_s << range.end.to_s
      command << "by#{sort_type}" if sort_type
      command << "rev" if reverse
      if limit
        offset, count = limit
        command << "limit" << offset << count
      end
      if withscores
        command << "withscores"
      end

      run command
    end

    enum SortType
      Score
      Lex
    end
  end
end
