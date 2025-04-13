require "redis"

module Conveyor
  class Scheduler
    getter? running = false
    getter interval : Time::Span
    getter jobs = [] of ScheduledJob
    @redis : Redis::Client
    @location : Time::Location = Time::Location::UTC

    def initialize(@redis, @interval = 1.second)
    end

    def weekly(
      job : Job,
      *,
      on weekdays : Array(Time::DayOfWeek),
      at time_of_day : String,
      in location : Time::Location = @location,
    ) : Nil
      weekly on: weekdays, at: time_of_day, in: location do
        job
      end
    end

    def weekly(
      *,
      on weekdays : Array(Time::DayOfWeek),
      at time_of_day : String,
      in location : Time::Location = @location,
      &block : -> Job
    ) : Nil
      hours, minutes = time_of_day.split(':', 2).map(&.to_i)
      now = Time.utc
      weekdays.each do |day|
        next_run = now.in(location).at_beginning_of_week + day.value.days + hours.hours + minutes.minutes
        if next_run < now
          next_run += 1.week
        end

        @jobs << ScheduledJob.new(next_run: next_run, interval: 1.week, &block)
      end
    end

    def daily(
      job : Job,
      *,
      at time_of_day : String,
      in location : Time::Location = @location,
    ) : Nil
      daily at: time_of_day, in: location do
        job
      end
    end

    def daily(
      *,
      at time_of_day : String,
      in location : Time::Location = @location,
      &block : -> Job
    ) : Nil
      hours, minutes = time_of_day.split(':', 2).map(&.to_i)
      now = Time.utc
      next_run = now.in(location).at_beginning_of_day + hours.hours + minutes.minutes
      if next_run < now
        next_run += 1.day
      end

      @jobs << ScheduledJob.new(next_run: next_run, interval: 1.day, &block)
    end

    def every(interval : Time::Span, job : Job, start_in : Time::Span = rand(interval.total_seconds).seconds) : Nil
      every interval, start_in do
        job
      end
    end

    def every(interval : Time::Span, start_in : Time::Span = rand(interval.total_seconds).seconds, &block : -> Job)
      @jobs << ScheduledJob.new(next_run: start_in.from_now, interval: interval, &block)
    end

    def start : Nil
      @running = true

      spawn do
        start = Time.monotonic
        primary = false

        while running?
          sleep interval
          start = Time.monotonic

          if primary
            @redis.expire "conveyor:lock:scheduler", interval * 2
          else
            if primary = check_primary
              Log.for("conveyor.scheduler").info { "taking over recurring job scheduling duties" }
            end
          end
          tick primary
        end
      end
    end

    def check_primary
      !!@redis.set("conveyor:lock:scheduler", "", ex: {interval * 2, 1.millisecond}.max, nx: true)
    end

    def tick(enqueue : Bool)
      now = Time.utc
      has_run = false

      if enqueue
        Log.for("conveyor.scheduler").trace { "tick" }
      end

      @jobs.each do |scheduled_job|
        if scheduled_job.next_run <= now
          # There may be multiple Conveyor instances running. If so, we only
          # want one to enqueue the jobs. That one is the active scheduler.
          if enqueue
            scheduled_job.enqueue
          end

          # Regardless of whether we're the active scheduler, we still need to
          # reschedule them in our own instance in case we *become* the active
          # scheduler during a failover, redeployment, etc.
          scheduled_job.reschedule

          # Make sure we flag that we've enqueued at least one job, so we will
          # need to re-sort the jobs by newly scheduled time.
          has_run = true
        end
      end
    end

    def stop
      @running = false
    end

    def time_zone(@location : Time::Location)
    end

    class ScheduledJob
      getter next_run : Time
      getter interval : Time::Span

      def initialize(@next_run, @interval, &@job : -> Job)
      end

      def job
        @job.call
      end

      def enqueue
        job.enqueue
      end

      def reschedule : Nil
        reschedule at: next_run + interval
      end

      def reschedule(at @next_run) : Nil
      end
    end
  end
end
