require "spec"

Conveyor.configure do |c|
  c.redis = Redis::Client.new
end
