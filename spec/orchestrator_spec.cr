require "./spec_helper"
require "../src/orchestrator"

Log.setup :debug

module ConveyorTestJobs
  struct Example < Conveyor::Job
    getter enqueued_at = Time.utc

    def initialize
    end

    def call
    end
  end
end

describe Conveyor::Orchestrator do
  orchestrator = Conveyor::Orchestrator.new
  redis = Redis::Client.new

  it "scans for orphan jobs" do
    job = ConveyorTestJobs::Example.new.enqueue
    key = "conveyor:job:#{job.conveyor_job_id}"
    begin
      pp redis.hgetall(key)
      redis.pipeline do |pipe|
        # Make it look like it's been picked up by setting pending=true and
        # removing it from the queue's list
        pipe.hset key, pending: "true"
        pipe.lrem "conveyor:queue:default", 1, job.conveyor_job_id
      end

      orchestrator.scan_for_orphans(
        lock_duration: 1.millisecond,
      )

      pp redis.hgetall(key)
    ensure
      job.dequeue
    end
  end
end
