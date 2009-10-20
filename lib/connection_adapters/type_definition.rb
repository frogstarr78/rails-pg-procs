module ActiveRecord
  module ConnectionAdapters
    class TypeDefinition < Struct.new(:id, :name, :columns)

      def to_sql(action="create", options={})
        case action
          when "create", :create
              "CREATE SCHEMA #{name.to_sql_name} AUTHORIZATION #{owner.to_sql_name}"
          # TODO - [ schema_element ]
          when "drop", :drop
            "DROP SCHEMA #{name.to_sql_name} #{cascade_or_restrict(options[:cascade])}"
          # TODO - [ IF EXISTS ]
        end
      end
    end
  end
end
