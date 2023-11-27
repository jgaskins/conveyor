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

    def self.new(hash : Hash)
      new(
        id: hash["id"].as(String),
        type: hash["type"].as(String),
        queue: hash["queue"].as(String),
        attempts: hash["attempts"].as(String).to_i32,
        payload: hash["payload"].as(String),
      )
    end

    def initialize(@id, @type, @queue, @attempts, @payload)
    end

    def job
      if handler = Job.handler_for(type)
        handler.from_json payload
      else
        raise UnknownJobType.new("No job type registered for #{type.inspect}")
      end
    end
  end
end
