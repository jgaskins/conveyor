require "./belt"
require "./configuration"
require "./scheduler"

module Conveyor
  class Orchestrator
    @config : Configuration
    @belts : Array(Belt)
    @running = false

    def initialize(configuration @config = CONFIG, @log = Log.for("conveyor"))
      @scheduler = Scheduler.new(redis: config.redis)
      @belts = Array.new(config.concurrency) do
        Belt.new(
          redis: config.redis,
          queues: config.queues,
          presence_duration: config.orphan_check_interval * 2,
          log: @log,
          max_attempts: config.max_attempts,
        )
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
      @scheduler.start

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
      @scheduler.stop
      @running = false
      @belts.each(&.stop)
    end

    @on_error : (::Exception -> Nil) = ->(ex : ::Exception) {}

    def on_error(&@on_error : ::Exception -> Nil) : self
      @belts.each(&.on_error(&on_error))
      self
    end

    def schedule : Nil
      yield @scheduler
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
      interval = @config.orphan_check_interval

      while @running
        sleep interval

        begin
          scan_for_orphans lock_duration: interval - 1.millisecond
        rescue ex
          @on_error.call ex
        end
      end
    end

    # :nodoc:
    def scan_for_orphans(lock_duration : Time::Span)
      if @config.redis.set("conveyor:lock:orphan-check", "", nx: true, ex: lock_duration)
        @log.info { "Checking for orphans" }

        scan_for_orphans!

        @log.info { "Orphan scan complete" }
      end
    end

    # :nodoc:
    def scan_for_orphans!
      redis = @config.redis

      belt_id_cache = Hash(String, Bool).new do |cache, key|
        cache[key] = redis.exists("conveyor:belt:#{key}") == 1
      end
      now = Time.utc.to_unix_ms

      # Begin crawling through Redis for Conveyor jobs
      redis.scan_each match: "conveyor:job:*" do |key|
        # Only jobs that have been scheduled or picked up by a belt count as
        # orphans, and we also need to know which queue to enqueue it under
        # if it is indeed an orphan, so we select only those fields.
        if raw_job_data = redis.hmget(key, "queue", "pending", "belt").as?(Array)
          queue, pending, belt_id = raw_job_data

          # Job ids are "conveyor:job:#{id}"
          id = key.lchop "conveyor:job:"

          # If it was set as pending and it has a belt id that no longer
          # matches a running belt, then the job is orphaned and needs to be
          # requeued in its original queue.
          if pending && belt_id && belt_id_cache[belt_id] == false
            redis.rpush "conveyor:queue:#{queue}", id
          end
        end
      end
    end

    private def check_for_scheduled
      interval = 1.second

      while @running
        sleep interval

        if @config.redis.set("conveyor:lock:schedule-check", "", nx: true, ex: interval - 10.milliseconds)
          begin
            enqueue_scheduled_jobs!
          rescue ex
            @on_error.call ex
          end
        end
      end
    end

    # :nodoc:
    def enqueue_scheduled_jobs!(from start_time = "-inf", until end_time = Time.utc)
      redis = @config.redis

      # TODO: Optimize this entire method to reduce heap allocations, because
      # if we have a massive backlog of scheduled jobs, this will involve a lot
      # of allocations.
      ids = redis
        .zrange("conveyor:scheduled", start_time..end_time, by: :score)
        .as(Array)
        .map(&.as(String))

      return if ids.empty?

      ids_and_queues = redis.pipeline do |pipe|
        ids.each do |id|
          pipe.hmget("conveyor:job:#{id}", "id", "queue")
        end
      end

      # Ignore any jobs that don't have their `id` and `queue` property set.
      # In an ideal world, this wouldn't ever actually remove anything, but
      # we're just being cautious.
      ids_and_queues = ids_and_queues.compact_map do |results|
        id, queue = results.as(Array)
        if id.is_a?(String) && queue.is_a?(String)
          [id, queue]
        end
      end

      ids_by_queue = ids_and_queues.each_with_object({} of String => Array(String)) do |(id, queue), hash|
        hash[queue] ||= [] of String
        hash[queue] << id
      end

      redis.multi do |txn|
        ids_by_queue.each do |(queue, ids)|
          txn.rpush "conveyor:queue:#{queue}", ids
        end

        txn.zrem "conveyor:scheduled", ids
      end
    end

    def clear_queues!
      if belt = @belts.first?
        belt.clear_queues!
      else
        raise NoBelts.new("There are no Conveyor belts defined")
      end
    end
  end
end

module Redis
  module Commands::SortedSet
    def zrange(
      key : String,
      range : Range(Value | Time, Value | Time),
      *,
      by sort_type : SortType? = nil,
      rev reverse : Bool? = nil,
      limit : {Int64, Int64}? = nil,
      withscores : Bool? = nil
    )
      low = case value = range.begin
            in Value
              value
            in Time
              value.to_unix_ms
            end
      high = case value = range.end
             in Value
               value
             in Time
               value.to_unix_ms
             end

      command = Array(String).new(initial_capacity: 10)
      command << "zrange" << key << low.to_s << high.to_s
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

  module Commands::List
    def rpush(key : String, values : Enumerable(String))
      command = Array(String).new(initial_capacity: 2 + values.size)
      command << "rpush" << key
      values.each { |value| command << value }

      run command
    end

    def lpos(key : String, value : String, *, rank : String | Int | Nil = nil, count : String | Int | Nil = nil, maxlen : String | Int | Nil = nil)
      command = Array(String).new(initial_capacity: 9)
      command << "lpos" << key << value
      command << "rank" << rank.to_s if rank
      command << "count" << count.to_s if count
      command << "maxlen" << maxlen.to_s if maxlen

      run command
    end
  end
end
