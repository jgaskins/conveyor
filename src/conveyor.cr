require "redis"

require "./configuration"
require "./errors"
require "./belt"
require "./orchestrator"

# TODO: Write documentation for `Conveyor`
module Conveyor
  VERSION = "0.1.0"

  class_getter orchestrator : Orchestrator { Orchestrator.new(CONFIG) }
end
