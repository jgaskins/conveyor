require "./spec_helper"

require "../src/belt"

module ConveyorTestJobs
  abstract struct BeltExample < Conveyor::Job
    queue "test1"
  end

  struct SingleJobExample < BeltExample
    # Track job runs
    class_getter jobs = Array(Time::Span).new

    def call
      self.class.jobs << Time.monotonic
    end
  end

  record QueuePriorityExample < BeltExample, name : String do
    def call
    end
  end

  record FailureExample < BeltExample, message : String = "hell" do
    def call
      raise message
    end
  end

  struct RetriesExhaustedExample < BeltExample
    max_attempts 1
    handle "ConveyorTestJobs::BeltExample", self

    def call
      raise "hell"
    end
  end
end

describe Conveyor::Belt do
  redis = Redis::Client.new
  queues = %w[test1 test2 test3]
  belt = Conveyor::Belt.new(
    redis: redis,
    queues: queues,
    presence_duration: 1.millisecond,
  )

  # Clear out the test queues so leftover state from old runs doesn't interfere
  before_each { belt.clear_queues! }
  after_all { belt.clear_queues! }

  10.times do
    it "runs a single job" do
      ConveyorTestJobs::SingleJobExample.jobs.clear
      id = ConveyorTestJobs::SingleJobExample.new.enqueue

      belt.run_one

      ConveyorTestJobs::SingleJobExample.jobs.should_not be_empty
    end
  end

  # Run the test a bunch of times to ensure a very low possibility of false positives
  100.times do
    it "prioritizes jobs from queues earlier in the belt's queue list" do
      lower_priority = ConveyorTestJobs::QueuePriorityExample.new("lower").enqueue(queue: "test2")
      higher_priority = ConveyorTestJobs::QueuePriorityExample.new("higher").enqueue(queue: "test1")

      if job_data = belt.fetch
        job_data
          .job.as(ConveyorTestJobs::QueuePriorityExample)
          .name
          .should eq "higher"
      else
        raise "Expected job data, got #{job_data.inspect}"
      end
    end
  end

  context "on failure" do
    it "increments the job's attempts counter" do
      id = ConveyorTestJobs::FailureExample.new.enqueue

      belt.run_one

      redis.hget("conveyor:job:#{id}", "attempts").should eq "1"
    end

    it "removes pending and belt fields from the job hash" do
      id = ConveyorTestJobs::FailureExample.new(message: "oops").enqueue

      belt.run_one

      redis.hmget("conveyor:job:#{id}", "id", "pending", "belt", "error")
        .should eq [id, nil, nil, "#<Exception:oops>"]
    end

    it "moves a job to the dead-job set after max_attempts are exceeded" do
      id = ConveyorTestJobs::RetriesExhaustedExample.new.enqueue

      belt.run_one

      redis.sismember("conveyor:dead", id).should eq 1
      redis.exists("conveyor:dead-job:#{id}").should eq 1
      redis.exists("conveyor:job:#{id}").should eq 0
    end
  end
end
