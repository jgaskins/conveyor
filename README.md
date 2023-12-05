# conveyor

Conveyor is a Crystal background-job framework, allowing you to offload work to other processes. This is useful for things like sending emails in web applications to avoid blocking the response while guaranteeing that the email is sent.

## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     conveyor:
       github: jgaskins/conveyor
   ```

2. Run `shards install`

## Usage

In your Conveyor configuration file (for example, in `src/config/conveyor.cr`):

```crystal
require "conveyor"
require "redis"

Conveyor.configure do |c|
  # The Redis client to use to store and retrieve your Conveyor jobs. Can also
  # be specified via the `REDIS_URL` env var. Either this option or the env var
  # must be provided.
  c.redis = Redis::Client.new(URI.parse("redis:///?max_idle_pool_size=10"))

  # How many concurrent workers the Conveyor orchestrator should start. Can be
  # set via CONVEYOR_CONCURRENCY env var, defaults to 10.
  c.concurrency = 10

  # Set your queues in priority order, defaults to the CONVEYOR_QUEUES env var
  # holding comma-separated queue names. If that is unspecified, it only
  # consumes the "default" queue.
  c.queues = %w[critical high default low]
end
```

### Enqueuing jobs

Jobs are `struct`s that inherit `Conveyor::Job` with a `call` method. All instance variables of a job must be serializable back and forth to JSON.

```crystal
struct ExampleJob < Conveyor::Job
  def initialize(@name : String)
  end

  def call
    # Do some work
  end
end
```

To enqueue this job, call `enqueue` on an instance of it:

```crystal
ExampleJob.new(name: "Foo").enqueue
```

### Scheduling jobs

To schedule the job to run at a later time, you can specify `in` or `at` arguments:

```crystal
ExampleJob.new(name: "Time::Span").schedule in: 1.minute
ExampleJob.new(name: "Time").schedule at: 5.minutes.from_now
```

### Recurring jobs

Recurring jobs can be scheduled on the orchestrator:

```crystal
struct MyDailyJob < Conveyor::Job
  def call
    # ...
  end
end

Conveyor.orchestrator.schedule do |schedule|
  schedule.daily MyDailyJob.new, at: "06:15", in: Time::Location::UTC
  schedule.every 30.seconds, Poll
end
```

Multiple instances of your workers will coordinate so you won't get multiple instances of each one.

## Setting queue

To set a job's default queue, define a `queue` method on it. If you don't specify one, it will be `"default"`.

```crystal
struct ExampleJob < Conveyor::Job
  # ...

  def queue
    "low"
  end
end
```

To enqueue a specific job on a different queue from the one its type defaults to (for example, to give specific invocations higher priority), you can pass a `queue` argument to `enqueue`:

```crystal
ExampleJob.new(name: "Foo").enqueue queue: "high"
```

### Processing jobs

In some job processors written in languages like Ruby that can load code at runtime, you might run a CLI provided by the framework to process jobs. Since Crystal doesn't allow runtime code loading, you need to write that yourself:

```crystal
require "./config/conveyor"
require "./jobs/**"

# Make sure you set up the orchestrator to shut down cleanly when your shutdown
# criteria are met. In this example, we're using TERM and INT signals to notify
# the background job workers to shut down.
[
  Signal::TERM,
  Signal::INT,
].each &.trap { Conveyor.orchestrator.stop }

Conveyor.orchestrator.start
```

## Development

idk i'm still figuring this out tbh

## Contributing

1. Fork it (<https://github.com/jgaskins/conveyor/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [Jamie Gaskins](https://github.com/jgaskins) - creator and maintainer
