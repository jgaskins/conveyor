require "../src/conveyor"
require "../src/job"

Conveyor.configure do |c|
  c.redis = Redis::Client.new(URI.parse("redis:///?max_idle_pool_size=10"))
  c.concurrency = 10
end

struct ExampleJob < Conveyor::Job
  getter string : String

  def initialize(@string)
  end

  def call
    # pp latency: Time.utc - Time::Format::RFC_3339.parse(string)
  end
end
