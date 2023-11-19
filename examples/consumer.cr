require "../src/conveyor"

require "./common"

Log.setup_from_env default_level: :info

orchestrator = Conveyor.orchestrator

Log.for("conveyor").notice { "starting" }
[
  Signal::TERM,
  Signal::INT,
].each &.trap { orchestrator.stop }

orchestrator
  .on_error { |ex| pp ex }
  .start
