module ActiveRecord
  module ConnectionAdapters
    class TypeDefinition < Struct.new(:id, :name, :columns); end
  end
end
