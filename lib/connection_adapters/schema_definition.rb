module ActiveRecord
  module ConnectionAdapters
    class SchemaDefinition < Struct.new(:name, :owner)
      include SchemaProcs

      def to_rdl
        "  create_schema(#{Inflector.symbolize(name)}, #{owner.to_sql_name})"
      end

#      CREATE SCHEMA schemaname [ AUTHORIZATION username ] [ schema_element [ ... ] ]
#      DROP SCHEMA [ IF EXISTS ] name [, ...] [ CASCADE | RESTRICT ]
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
