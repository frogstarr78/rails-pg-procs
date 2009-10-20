module ActiveRecord
  module ConnectionAdapters
    class ProcedureDefinition < Struct.new(:id, :name)
    end
  end
end
