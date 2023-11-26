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

  it "reenqueues orphan jobs" do
    job = ConveyorTestJobs::Example.new.enqueue
    key = "conveyor:job:#{job.conveyor_job_id}"
    begin
      # Make it look like it's been picked up by setting pending=true,
      # setting the job's belt_id, and removing its id from the queue's list
      redis.hset key, pending: "true", belt_id: UUID.random.to_s
      redis.lrem "conveyor:queue:default", 1, job.conveyor_job_id

      orchestrator.scan_for_orphans!

      redis.run({"lpos", "conveyor:queue:default", job.conveyor_job_id})
        .should_not eq nil
    ensure
      job.dequeue
    end
  end

  it "ignores jobs without pending == true when scanning for orphans" do
    job = ConveyorTestJobs::Example.new.enqueue
    key = "conveyor:job:#{job.conveyor_job_id}"
    begin
      redis.hset key,  belt_id: UUID.random.to_s
      redis.lrem "conveyor:queue:default", 1, job.conveyor_job_id

      orchestrator.scan_for_orphans!

      redis.run({"lpos", "conveyor:queue:default", job.conveyor_job_id})
        .should eq nil
    ensure
      job.dequeue
    end
  end

  it "does not count jobs with an existing belt as orphans" do
    job = ConveyorTestJobs::Example.new.enqueue
    key = "conveyor:job:#{job.conveyor_job_id}"
    belt_id = UUID.random.to_s

    begin
      redis.hset key, pending: "true", belt_id: belt_id
      redis.set "conveyor:belt:#{belt_id}", "", ex: 1.second # belt is present, so it is working on this job
      redis.lrem "conveyor:queue:default", 1, job.conveyor_job_id

      orchestrator.scan_for_orphans!

      redis.run({"lpos", "conveyor:queue:default", job.conveyor_job_id})
        .should eq nil
    ensure
      job.dequeue
    end
  end

  it "does not enqueue scheduled jobs which are not yet ready" do
    job = ConveyorTestJobs::Example.new.enqueue in: 1.minute
    key = "conveyor:job:#{job.conveyor_job_id}"

    begin
      orchestrator.enqueue_scheduled_jobs

      redis.run({"lpos", "conveyor:queue:default", job.conveyor_job_id})
        .should eq nil
    ensure
      job.dequeue
    end
  end

  it "enqueues scheduled jobs" do
    job = ConveyorTestJobs::Example.new.enqueue in: -1.minute
    key = "conveyor:job:#{job.conveyor_job_id}"

    begin
      orchestrator.enqueue_scheduled_jobs

      redis.run({"lpos", "conveyor:queue:default", job.conveyor_job_id})
        .should_not eq nil
    ensure
      job.dequeue
    end
  end
end
