module ActiveRecord
  module ConnectionAdapters # :nodoc:
    module SchemaStatements
      include SchemaProcs
      def drop_table name, options={}
        execute "DROP TABLE #{name.inspect} #{cascade_or_restrict(options[:cascade])}" 
      end
    end
  end
end

