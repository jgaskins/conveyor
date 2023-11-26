require "json"
require "uuid/json"
require "./configuration"

module Conveyor
  abstract struct Job
    include JSON::Serializable

    # :nodoc:
    getter conveyor_job_id : String { generate_id }

    abstract def call

    private REGISTRY = {} of String => Job.class

    macro inherited
      handle {{@type.stringify}}, {{@type}}
    end

    macro handle(type_name, type)
      Conveyor::Job.register {{type_name.id.stringify}}, {{type}}

      def conveyor_job_type
        {{type_name}}
      end
    end

    def self.register(type_name : String, type : Job.class)
      REGISTRY[type_name] = type
    end

    def self.handler_for(type_name : String)
      REGISTRY[type_name]?
    end

    def enqueue(in delay : Time::Span, queue : String = self.queue, configuration config : Configuration = CONFIG)
      enqueue at: delay.from_now
    end

    def enqueue(at time : Time, queue : String = self.queue, configuration config : Configuration = CONFIG) : self
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

    def enqueue(*, queue : String = self.queue, configuration config : Configuration = CONFIG) : self
      config.redis.pipeline do |pipe|
        pipe.hset "conveyor:job:#{conveyor_job_id}",
          id: conveyor_job_id,
          type: conveyor_job_type,
          queue: queue,
          payload: to_json
        pipe.rpush "conveyor:queue:#{queue}", conveyor_job_id
      end

      self
    end

    def dequeue(*,  configuration config : Configuration = CONFIG) : self
      # We don't need to remove the job from any queues. When the job fetcher
      # retrieves the job data, if the key doesn't exist it will ignore it and
      # move onto the next job. This makes dequeuing an O(1) operation.
      config.redis.unlink "conveyor:job:#{conveyor_job_id}"
      self
    end

    def unschedule(configuration config : Configuration = CONFIG)
      config.redis.zrem "conveyor:scheduled", conveyor_job_id

    end

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
