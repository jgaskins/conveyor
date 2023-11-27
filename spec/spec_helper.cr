require "spec"

Conveyor.configure do |c|
  c.redis = Redis::Client.new
  c.queues = %w[test1 test2 test3]
end
