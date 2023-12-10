require "json"
require "uuid/json"
require "./configuration"

module Conveyor
  # The `Job` is the unit of work in Conveyor. A representation of it is stored
  # in Redis along with some metadata, and when it is time to execute that job,
  # it is deserialized and its `call` method is invoked.
  #
  # Jobs are enqueued to be executed ASAP with the `enqueue` method or scheduled
  # with the `schedule` method. Enqueuing a job runs it as soon as there is an
  # available `Conveyor::Belt`.
  #
  # Defining a job is a matter of defining a struct that inherits from
  # `Conveyor::Job` (either directly or indirectly) and overriding the `call`
  # method to perform its work. The `call` method is used as the calling
  # convention used by `Proc` instances.
  abstract struct Job
    include JSON::Serializable

    def initialize
    end

    # Override this method to provide this job's functionality.
    abstract def call

    private REGISTRY = {} of String => Job.class

    macro inherited
      handle {{@type.stringify}}, {{@type}}
    end

    # :nodoc:
    macro handle(type_name, type)
      Conveyor::Job.register {{type_name.id.stringify}}, {{type}}

      def conveyor_job_type
        {{type_name}}
      end
    end

    # :nodoc:
    def self.register(type_name : String, type : Job.class)
      REGISTRY[type_name] = type
    end

    # :nodoc:
    def self.handler_for(type_name : String)
      REGISTRY[type_name]?
    end

    # Enqueues this job in the specified queue, or to the job's default queue if
    # the queue isn't provided, to be executed after the given amount of time.
    # You can also pass a `Configuration` instance to use different settings.
    #
    # ```
    # struct MyJob < Conveyor::Job
    #   def initialize(@arg : String)
    #   end
    #
    #   def call
    #     # ...
    #   end
    # end
    #
    # MyJob.new("asdf").enqueue in: 1.minute
    # ```
    def schedule(in delay : Time::Span, queue : String = self.queue, configuration config : Configuration = CONFIG) : String
      schedule at: delay.from_now, queue: queue, configuration: config
    end

    # Enqueues this job in the specified queue, or to the job's default queue if
    # the queue isn't provided explicitly, to be executed at the specified
    # time. You can also pass a `Configuration` instance to use different
    # settings.
    #
    # ```
    # struct MyJob < Conveyor::Job
    #   def initialize(@arg : String)
    #   end
    #
    #   def call
    #     # ...
    #   end
    # end
    #
    # MyJob.new("asdf").enqueue at: timestamp
    # ```
    def schedule(at time : Time, queue : String = self.queue, configuration config : Configuration = CONFIG) : String
      id = generate_id

      config.redis.pipeline do |pipe|
        pipe.hset "conveyor:job:#{id}",
          id: id,
          type: conveyor_job_type,
          queue: queue,
          payload: to_json
        pipe.zadd "conveyor:scheduled",
          score: time.to_unix_ms,
          value: id
      end

      id
    end

    # Enqueues this job in the specified queue, or to the job's default queue if
    # the queue isn't provided explicitly, to be executed immediately. You can
    # also pass a `Configuration` instance to use different settings.
    #
    # ```
    # struct MyJob < Conveyor::Job
    #   def initialize(@arg : String)
    #   end
    #
    #   def call
    #     # ...
    #   end
    # end
    #
    # MyJob.new("asdf").enqueue
    # ```
    def enqueue(*, queue : String = self.queue, configuration config : Configuration = CONFIG) : String
      id = generate_id

      result = config.redis.pipeline do |pipe|
        pipe.hset "conveyor:job:#{id}",
          id: id,
          type: conveyor_job_type,
          queue: queue,
          payload: to_json
        pipe.rpush "conveyor:queue:#{queue}", id
      end

      id
    end

    # Remove this job from all queues. This method will not unschedule the job.
    def self.dequeue(id : String, *, configuration config : Configuration = CONFIG) : Nil
      # We don't need to remove the job from any queues. When the job fetcher
      # retrieves the job data, if the key doesn't exist it will ignore it and
      # fetch the next job. This makes dequeuing an O(1) operation.
      config.redis.unlink "conveyor:job:#{id}"
    end

    # If this job was scheduled via one of the `schedule` methods, this method will remove it from the schedule.
    def self.unschedule(id : String, configuration config : Configuration = CONFIG) : Nil
      config.redis.zrem "conveyor:scheduled", id
    end

    # :nodoc:
    def queue
      "default"
    end

    # :nodoc:
    def generate_id
      UUID.random.to_s
    end

    # Define the queue your job will enqueue on by default
    #
    # ```
    # struct SendEmail < Conveyor::Job
    #   # Emails get their own queue
    #   queue "email"
    #
    #   def initialize(@email_address : String, @body : String)
    #   end
    #
    #   def call
    #     # ...
    #   end
    # end
    # ```
    macro queue(name)
      def queue
        "{{name.id}}"
      end
    end

    class_getter max_attempts : Int32 = 25

    def self.max_attempts(@@max_attempts : Int32)
    end
  end

  # You can use this job type for testing empty jobs, such as benchmarking your
  # job-processing infrastructure (compute infra and Redis).
  struct Bogus < self
    def call
    end
  end

  # This is provided just to make sure we have at least 2 Job subclasses. As of
  # Crystal 1.10.1, it crashes the compiler if there aren't.
  private struct Bogus2 < self
    def call
    end
  end
end
