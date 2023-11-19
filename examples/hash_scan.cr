require "redis"

redis = Redis::Client.new

redis.pipeline do |pipe|
  1_000.times do |i|
    pipe.hset "my-hash", i.to_s, "value"
  end
end

redis.hscan_each "my-hash" do |field|
  pp field
end
