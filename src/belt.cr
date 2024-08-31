require "redis"
require "./job_data"
require "./job"
require "./errors"

module Conveyor
  # The `Conveyor::Belt` processes your `Job` instances sequentially. Each job
  # is fetched from Redis and placed on the belt to be processed. If the job
  # does not complete successfully, it is removed from the belt and placed back
  # in the queue, waiting an [exponential amount of
  # time](https://en.wikipedia.org/wiki/Exponential_backoff) before retrying.
  #
  # Your application can have one or more `Conveyor::Belt`s, allowing you to process multiple jobs concurrently. You can configure this by setting `Configuration#concurrency`:
  #
  # ```
  # Conveyor.configure do |config|
  #   # ...
  #   c.concurrency = 10
  # end
  # ```
  #
  # The `Orchestrator` will manage your `Belt` instances throughout their entire
  # lifecycle, so you shouldn't need to deal with `Conveyor::Belt` directly in
  # your application, but it's a good idea to know that it exists.
  class Belt
    getter? running = false
    getter state : State = :waiting
    getter id = UUID.random.to_s

    @redis : Redis::Client
    @queues : Array(String)
    @on_error : Proc(::Exception, Nil) = ->(ex : ::Exception) {}
    @jobs = Deque(Time::Span).new
    @presence_duration : Time::Span
    @max_attempts : Int32

    def initialize(
      *,
      @redis,
      @queues,
      @presence_duration,
      @timeout = 2.seconds,
      @log = Log.for("conveyor.belt"),
      @max_attempts = 25
    )
    end

    # Start up this belt to begin processing jobs from the queues that feed
    # into it. This is called by the `Orchestrator` on start.
    def start
      @running = true

      spawn do
        while running?
          begin
            # Ensure we refresh our presence key so the orchestrator knows we're
            # still here
            @redis.set "conveyor:belt:#{id}", ".", ex: @presence_duration
          rescue ex
          ensure
            sleep @presence_duration - 100.milliseconds
          end
        end
      end

      while running?
        begin
          run_one
        rescue ex
          begin
            @on_error.call ex
          rescue secondary_failure
            # If the error handler raises, wtf do we even do?
            @log.error(exception: secondary_failure) { secondary_failure }
          end
        end
      end
    ensure
      yield
    end

    # Fetch and perform a single job
    def run_one : Nil
      @state = :waiting
      if job_data = fetch
        @state = :working
        work job_data
      end
    ensure
      @state = :waiting
    end

    # Stop processing jobs on this belt. The currently processing job will finish as long as the process does not exit beforehand, but no new jobs will be processed on this belt.
    #
    # The `Orchestrator` typically calls this method.
    def stop
      @running = false
    end

    # Tell this belt what to do when an exception occurs while `fetch`ing or
    # `work`ing on a `Job`.
    def on_error(&@on_error : ::Exception -> Nil)
      self
    end

    # Retrieves `JobData` from Redis, if there are any pending jobs in any of
    # the queues passed to this `Belt`'s constructor. This method also marks the
    # job in Redis as `pending` and assigned to this `Belt`.
    def fetch : JobData?
      queues = @queues.map do |name|
        "conveyor:queue:#{name}"
      end
      start = Time.monotonic
      if result = @redis.blpop(queues, timeout: @timeout).as?(Array)
        queue, job_id = {String, String}.from result
        @log.trace &.emit "Received job",
          poll_duration_sec: (Time.monotonic - start).total_seconds,
          job_id: job_id

        fetch_job_data(job_id, queue.lchop("conveyor:queue:"))
      end
    end

    # :nodoc:
    def fetch_job_data(id : String, queue : String)
      key = "conveyor:job:#{id}"

      type, attempts, payload = @redis
        .hmget(key, "type", "attempts", "payload")
        .as(Array)

      if type && payload
        attempts ||= 0
        @redis.hset key,
          pending: "true",
          belt: id.to_s
        JobData.new(
          id: id,
          type: type.as(String),
          queue: queue,
          attempts: attempts.as(String | Int32).to_i,
          payload: payload.as(String),
        )
      end
    end

    # Deserializes the job payload provided by the given `JobData` and calls the
    # job type's `Job#call` method. If an exception occurs while processing the
    # job, the belt's `on_error` block is invoked and the job is rescheduled to
    # run on an exponential-backoff schedule based on how many times the job has
    # been attempted.
    def work(data : JobData)
      @log.debug &.emit "starting",
        id: data.id,
        type: data.type,
        queue: data.queue,
        attempts: data.attempts

      start = Time.monotonic
      begin
        clear_outdated_jobs!
        job = data.job

        work job
        delete data.id
        finish = Time.monotonic
        @log.info &.emit "complete",
          id: data.id,
          type: data.type,
          queue: data.queue,
          attempts: data.attempts + 1,
          duration_sec: (finish - start).total_seconds
      rescue ex
        @on_error.call ex
        errored_at = Time.monotonic
        max_attempts = data.job_type.max_attempts rescue 25

        @log.error exception: ex, &.emit "error",
          id: data.id,
          type: data.type,
          queue: data.queue,
          attempts: data.attempts + 1,
          max_attempts: max_attempts,
          duration_sec: (errored_at - start).total_seconds

        reenqueue data, max_attempts, ex
      ensure
        @jobs << start
      end
    end

    # :nodoc:
    def work(job : Job)
      job.call
    end

    # Reschedule the job to run after an amount of time based on the number of times the job has been attempted has passed.
    def reenqueue(job_data : JobData, max_attempts : Int32, exception : ::Exception?) : self
      job_key = "conveyor:job:#{job_data.id}"

      @redis.pipeline do |pipe|
        pipe.hincrby job_key, "attempts", "1"
        pipe.hdel job_key, "pending", "belt"
        if exception
          pipe.hset job_key, "error", exception.inspect
        end

        if job_data.attempts + 1 < max_attempts
          # Exponential backoff, up to 2**25 milliseconds (9 hours and change)
          scheduled_time = (1 << {job_data.attempts + 5, 25}.min)
            .milliseconds
            .from_now

          # pipe.rpush "conveyor:queue:#{job_data.queue}", job_data.id
          pipe.zadd "conveyor:scheduled",
            score: scheduled_time.to_unix_ms,
            value: job_data.id
        else
          # TODO: Add RENAME to the Redis shard
          pipe.run({"rename", job_key, "conveyor:dead-job:#{job_data.id}"})
          pipe.sadd "conveyor:dead", job_data.id
        end
      end

      self
    end

    def delete(id : String) : self
      @redis.unlink "conveyor:job:#{id}"
      self
    end

    def clear_queues!
      queue_keys = @queues.map { |queue| "conveyor:queue:#{queue}" }
      job_keys = @redis
        .pipeline do |pipe|
          queue_keys.each do |key|
            pipe.lrange(key, 0, -1)
          end
        end
        .flat_map(&.as(Array))
        .flat_map { |id| "conveyor:job:#{id}" }
      @redis.unlink job_keys + queue_keys
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
