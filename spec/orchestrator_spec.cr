require "./spec_helper"
require "../src/orchestrator"

Log.setup :debug

module ConveyorTestJobs
  struct OrchestratorExample < Conveyor::Job
    queue "test1"

    getter enqueued_at = Time.utc

    def call
    end
  end
end

describe Conveyor::Orchestrator do
  orchestrator = Conveyor::Orchestrator.new
  redis = Redis::Client.new

  before_each do
    orchestrator.scan_for_orphans!
    orchestrator.clear_queues!
  end

  it "reenqueues orphan jobs" do
    id = ConveyorTestJobs::OrchestratorExample.new.enqueue
    key = "conveyor:job:#{id}"
    begin
      # Make it look like it's been picked up by setting pending=true,
      # setting the job's belt_id, and removing its id from the queue's list
      redis.hset key, pending: "true", belt: UUID.random.to_s
      redis.lrem "conveyor:queue:test1", 1, id

      orchestrator.scan_for_orphans!

      redis.lpos("conveyor:queue:test1", id).should_not eq nil
    ensure
      Conveyor::Job.dequeue id
    end
  end

  it "ignores jobs without pending == true when scanning for orphans" do
    id = ConveyorTestJobs::OrchestratorExample.new.enqueue
    key = "conveyor:job:#{id}"
    begin
      redis.hset key, belt: UUID.random.to_s
      redis.lrem "conveyor:queue:test1", 1, id

      orchestrator.scan_for_orphans!

      redis.lpos("conveyor:queue:test1", id).should eq nil
    ensure
      Conveyor::Job.dequeue id
    end
  end

  it "does not count jobs with an existing belt as orphans" do
    id = ConveyorTestJobs::OrchestratorExample.new.enqueue
    key = "conveyor:job:#{id}"
    belt_id = UUID.random.to_s

    begin
      redis.hset key, pending: "true", belt: belt_id
      redis.set "conveyor:belt:#{belt_id}", "", ex: 1.second # belt is present, so it is working on this job
      redis.lrem "conveyor:queue:test1", 1, id

      orchestrator.scan_for_orphans!

      redis.lpos("conveyor:queue:test1", id)
        .should eq nil
    ensure
      Conveyor::Job.dequeue id
    end
  end

  it "enqueues scheduled jobs" do
    id = ConveyorTestJobs::OrchestratorExample.new.schedule in: -1.minute
    key = "conveyor:job:#{id}"

    begin
      orchestrator.enqueue_scheduled_jobs!

      redis.lpos("conveyor:queue:test1", id).should_not eq nil
    ensure
      Conveyor::Job.dequeue id
    end
  end

  it "does not enqueue scheduled jobs which are not yet ready" do
    id = ConveyorTestJobs::OrchestratorExample.new.schedule in: 1.minute
    key = "conveyor:job:#{id}"

    begin
      orchestrator.enqueue_scheduled_jobs!

      redis.lpos("conveyor:queue:test1", id).should eq nil
    ensure
      Conveyor::Job.dequeue id
    end
  end
end
