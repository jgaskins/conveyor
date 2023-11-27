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

    # :nodoc:
    getter conveyor_job_id : String { generate_id }

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
    def schedule(in delay : Time::Span, queue : String = self.queue, configuration config : Configuration = CONFIG) : self
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
    def schedule(at time : Time, queue : String = self.queue, configuration config : Configuration = CONFIG) : self
      config.redis.pipeline do |pipe|
        pipe.hset "conveyor:job:#{conveyor_job_id}",
          id: conveyor_job_id,
          type: conveyor_job_type,
          queue: queue,
          payload: to_json
        pipe.zadd "conveyor:scheduled",
          score: time.to_unix_ms,
          value: conveyor_job_id
      end

      self
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
    def enqueue(*, queue : String = self.queue, configuration config : Configuration = CONFIG) : self
      result = config.redis.pipeline do |pipe|
        pipe.hset "conveyor:job:#{conveyor_job_id}",
          id: conveyor_job_id,
          type: conveyor_job_type,
          queue: queue,
          payload: to_json
        pipe.rpush "conveyor:queue:#{queue}", conveyor_job_id
      end

      self
    end

    # Remove this job from all queues. This method will not unschedule the job.
    def dequeue(*, configuration config : Configuration = CONFIG) : self
      # We don't need to remove the job from any queues. When the job fetcher
      # retrieves the job data, if the key doesn't exist it will ignore it and
      # move onto the next job. This makes dequeuing an O(1) operation.
      config.redis.unlink "conveyor:job:#{conveyor_job_id}"
      self
    end

    # If this job was scheduled via one of the `schedule` methods, this method will remove it from the schedule.
    def unschedule(configuration config : Configuration = CONFIG) : self
      config.redis.zrem "conveyor:scheduled", conveyor_job_id
      self
    end

    # Override this method to
    def queue
      "default"
    end

    # :nodoc:
    def generate_id
      UUID.random.to_s
    end

    macro queue(name)
      def queue
        "{{name.id}}"
      end
    end
  end
end
