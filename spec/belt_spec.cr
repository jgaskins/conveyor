require "./spec_helper"

require "../src/belt"

module ConveyorTestJobs
  struct SingleJobExample < Conveyor::Job
    queue "test1"

    class_getter jobs = Array(Time::Span).new

    def call
      self.class.jobs << Time.monotonic
    end
  end

  struct QueuePriorityExample < Conveyor::Job
    queue "test1"

    getter name : String

    def initialize(@name)
    end

    def call
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

  it "runs a single job" do
    ConveyorTestJobs::SingleJobExample.jobs.clear
    job = ConveyorTestJobs::SingleJobExample.new.enqueue

    belt.run_one

    ConveyorTestJobs::SingleJobExample.jobs.should_not be_empty
  end

  # Run the test a bunch of times to ensure a very low possibility of false positives
  100.times do
    it "prioritizes jobs from queues earlier in the belt's queue list" do
      belt.clear_queues queues
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
end
