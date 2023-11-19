require "../src/conveyor"

require "./common"

Log.setup_from_env default_level: :info
loop do
  ExampleJob
    .new(string: Time.utc.to_rfc3339(fraction_digits: 9))
    .enqueue
  ExampleJob
    .new(string: Time.utc.to_rfc3339(fraction_digits: 9))
    .enqueue(in: 10.seconds)
  sleep 100.milliseconds
end

sleep
