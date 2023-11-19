module Conveyor
  class Exception < ::Exception
    macro define(name)
      class {{name}} < {{@type}}
      end
    end
  end

  Exception.define UnknownJobType
end
