module ActiveRecord
  module ConnectionAdapters
    class IndexDefinition < Struct.new(:table, :name, :unique, :columns)
      include SchemaProcs

      def to_rdl add=true, options={}
        return "  add_index #{table.inspect}, #{columns.inspect}, :name => #{name.inspect}#{', :unique => true' if unique || unique == 't'}" if add
#        "  remove_index #{table.inspect}, #{columns.inspect}, :name => #{name.inspect}#{', :unique => true' if unique || unique == 't'}" if add
      end

      def to_sql(action="create", options={})
        case action
          when "create", :create
              "CREATE INDEX #{name.to_sql_name}"
          # TODO - [ schema_element ]
          when "drop", :drop
            "DROP INDEX #{quote_column_name(index_name(table, options))} ON #{table}"
#            "DROP SCHEMA #{name.to_sql_name} #{cascade_or_restrict(options[:cascade])}"
          # TODO - [ IF EXISTS ]
        end
      end
    end
  end
end
