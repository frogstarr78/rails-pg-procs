module ActiveRecord
  module ConnectionAdapters
    class ViewDefinition
      include SchemaProcs
      attr_accessor :id, :name, :columns, :view_body
      def initialize(id, name, columns=[], &block)
        @id            = id
        self.name      = name
        self.columns   = columns
        self.view_body = block
      end

      def to_rdl
        "  create_view(#{ActiveSupport::Inflector.symbolize(name)}) { $#{name}_body$\n    #{view_body.call}\n  $#{name}_body$ }"
			end

#     CREATE [ OR REPLACE ] [ TEMP | TEMPORARY ] VIEW NAME [ ( column_name [, ...] ) ]
#     AS query
#     [ WITH [ CASCADED | LOCAL ] CHECK OPTION ]
#			DROP VIEW [ IF EXISTS ] NAME [, ...] [ CASCADE | RESTRICT ]
      def to_sql(action="create", options={})
        case action
          when "create", :create
            ret = "CREATE OR REPLACE#{' TEMPORARY' if options[:temp] } VIEW #{name.to_sql_name}
            AS #{view_body.call}"
				# TODO - [ WITH [ CASCADED | LOCAL ] CHECK OPTION ]
          when "drop", :drop
            ret = "DROP VIEW #{name.to_sql_name} #{cascade_or_restrict(options[:cascade])}"
        end
        ret
      end
    end
  end
end
