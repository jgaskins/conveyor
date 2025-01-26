module Conveyor
  class Exception < ::Exception
    # :nodoc:
    macro define(name)
      class {{name}} < {{@type}}
      end
    end
  end

  Exception.define UnknownJobType
  Exception.define NoBelts
end
