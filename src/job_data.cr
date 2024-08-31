require "./job"
require "./errors"

module Conveyor
  struct JobData
    getter id : String
    getter type : String
    getter queue : String
    getter attempts : Int32
    protected setter attempts
    getter payload : String
    getter? pending = false
    getter belt : String?

    def self.new(hash : Hash)
      new(
        id: hash["id"].as(String),
        type: hash["type"].as(String),
        queue: hash["queue"].as(String),
        attempts: hash.fetch("attempts", 0).as(String | Int32).to_i32,
        payload: hash["payload"].as(String),
        pending: hash["pending"]? == "true",
        belt: hash["belt"]?.as(String?),
      )
    end

    def initialize(@id, @type, @queue, @attempts, @payload, @pending = false, @belt = nil)
    end

    getter job : Job { job_type.from_json payload }

    def job_type
      if job_type = Job.handler_for(type)
        job_type
      else
        raise UnknownJobType.new("No job type registered for #{type.inspect}")
      end
    end
  end
end
