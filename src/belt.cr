require "redis"
require "./job_data"
require "./job"

module Conveyor
  class Belt
    getter? running = false
    getter state : State = :waiting
    getter id = UUID.random.to_s

    @redis : Redis::Client
    @queues : Array(String)
    @on_error : Proc(::Exception, Nil) = ->(ex : ::Exception) {}
    @jobs = Deque(Time::Span).new

    def initialize(@redis, @queues, @timeout = 2.seconds, @log = Log.for("conveyor.belt"))
    end

    def start
      @running = true

      while running?
        begin
          @state = :waiting
          if job_data = fetch
            @state = :working
            work job_data
          end
        rescue ex
          @on_error.call ex
        end
      end
    ensure
      yield
    end

    def stop
      @running = false
    end

    def on_error(&@on_error : ::Exception -> Nil)
      self
    end

    def fetch
      queues = @queues.map do |name|
        "conveyor:queue:#{name}"
      end
      start = Time.monotonic
      if result = @redis.blpop(queues, timeout: @timeout).as?(Array)
        @log.trace &.emit "Received job",
          poll_duration_sec: (Time.monotonic - start).total_seconds
        queue, job_id = result
        queue = queue.as(String)
        job_id = job_id.as(String)
        key = "conveyor:job:#{job_id}"
        if result = @redis.hmget(key, "id", "type", "queue", "attempts", "payload").as?(Array)
          id, type, queue, attempts, payload = result
          if id && type && queue && payload
            @redis.hset key,
              pending: "true",
              belt: object_id.to_s
            JobData.new({
              "id"       => id,
              "type"     => type,
              "queue"    => queue,
              "attempts" => attempts || "0",
              "payload"  => payload,
            })
          end
        else
          @log.info &.emit "Missing job", id: job_id, queue: queue
        end
      end
    end

    def work(data : JobData)
      if job_class = Job.handler_for(data.type)
        @log.debug &.emit "starting", id: data.id, type: data.type, queue: data.queue, attempts: data.attempts
        start = Time.monotonic
        begin
          clear_outdated_jobs!

          work job_class.from_json(data.payload)
          @jobs << start
          delete data.id
        rescue ex
          @on_error.call ex
          reenqueue data, ex
        ensure
          finish = Time.monotonic
          @log.info &.emit "complete", id: data.id, type: data.type, queue: data.queue, attempts: data.attempts, duration_sec: (finish - start).total_seconds
        end
      else
        raise UnknownJobType.new("No job type registered for #{data.type.inspect}")
      end
    end

    def work(job : Job)
      job.call
    end

    def reenqueue(job_data : JobData, exception : ::Exception?)
      @redis.pipeline do |pipe|
        job_key = "conveyor:job:#{job_data.id}"
        pipe.hincrby job_key, "attempts", "1"
        pipe.hdel "pending", "belt_id"
        pipe.rpush "conveyor:queue:#{job_data.queue}", job_data.id
        if message = exception.try(&.message)
          pipe.hset job_key, "error", message
        end
      end
    end

    def delete(id : String)
      @redis.unlink "conveyor:job:#{id}"
    end

    def jobs_per_second
      clear_outdated_jobs!
      @jobs.size
    end

    private def clear_outdated_jobs!
      now = Time.monotonic
      while (start = @jobs.first?) && now - start >= 1.second
        @jobs.shift?
      end
    end

    enum State
      Waiting
      Working
    end
  end
end
