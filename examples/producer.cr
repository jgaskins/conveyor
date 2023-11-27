require "../src/conveyor"

require "./common"

Log.setup_from_env default_level: :info
loop do
  job = ExampleJob.new(string: Time.utc.to_rfc3339(fraction_digits: 9))
  job.enqueue
  job.schedule in: 10.seconds

  sleep 1.second
end

# ErrorJob.new.enqueue

sleep
