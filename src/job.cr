require "json"
require "uuid/json"
require "./configuration"

module Conveyor
  abstract struct Job
    include JSON::Serializable

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

    def enqueue(at time : Time, queue : String = self.queue, configuration config : Configuration = CONFIG)
      id = generate_id.to_s
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
    end

    def enqueue(*, queue : String = self.queue, configuration config : Configuration = CONFIG)
      id = generate_id.to_s
      config.redis.pipeline do |pipe|
        pipe.hset "conveyor:job:#{id}",
          id: id,
          type: conveyor_job_type,
          queue: queue,
          payload: to_json
        pipe.rpush "conveyor:queue:#{queue}", id
      end
      id
    end

    def queue
      "default"
    end

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
