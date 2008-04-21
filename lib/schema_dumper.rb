module ActiveRecord
  # This class is used to dump the database schema for some connection to some
  # output format (i.e., ActiveRecord::Schema).
  class SchemaDumper
    include SchemaProcs

    # TODO -Implement checks on SchemaDumper instance 
    # to ensure we do this only when using pg db.
    def postgres?
      adapter_name == 'PostgreSQL'
    end

    private
      def get_type(types)
        case types
          when Array
            types.collect {|type|
              get_type(type)
            }.join(", ")
          when String && /^\d+$/
            type = @connection.select_value("SELECT typname FROM pg_type WHERE oid = '#{types}'")
            return type = 'nil' if type == 'void'
            get_type(type)
          when String
            return %("#{types}") if types =~ /[\s\(]/
            Inflector.symbolize(types)
        end
      end

      # TODO - Facilitate create_proc(name, [argname, argtype] and create_proc(name, [argmode, argname, argtype] ...
      def procedures(stream, conditions=nil)
        @connection.procedures(conditions).each { |proc_row|
          oid, name, namespace, owner, lang, is_agg, sec_def, is_strict, ret_set, volatile, nargs, ret_type, arg_types, arg_names, src, bin, acl = proc_row
          is_agg    = is_agg    == 't'
          is_strict = is_strict == 't'
          ret_set   = ret_set   == 't'
          arg_names ||= ''
          args      = get_type(arg_types.split(" "))#.zip(arg_names.split(" "))

          stream.print "  create_proc(#{Inflector.symbolize(name)}, [#{args}], :return => #{get_type(ret_type)}"
          stream.print ", :resource => ['#{bin}', '#{src}']" unless bin == '-'
          stream.print ", :set => true" if ret_set
          stream.print ", :strict => true" if is_strict
          stream.print ", :behavior => '#{behavior(volatile)}'" unless volatile == 'v'
          stream.print ", :lang => '#{lang}')"
          stream.print " {\n    <<-#{Inflector.underscore(name)}_sql\n#{src.chomp}\n    #{Inflector.underscore(name)}_sql\n  }" if bin == '-'
          stream.print "\n"
        }
      end

      def triggers(table_name, stream)
        triggers = @connection.triggers(table_name)
        triggers.each {|trigger|
          stream.puts trigger.to_rdl
        }
        stream.puts unless triggers.empty?
      end

      def types(stream)
        @connection.types.each {|type|
          stream.print "  create_type(#{Inflector.symbolize(type.name)}, "
          stream.print "#{ type.columns.collect{|column, type| "[#{Inflector.symbolize(column)}, #{get_type(type)}]"}.join(", ") }"
          stream.puts  ")"
        }
      end

      alias_method :procless_tables, :tables
      def tables(stream)
        types(stream)
        procedures(stream, "!= 'sql'")
        procless_tables(stream)
        procedures(stream, "= 'sql'")
      end

      alias_method :indexes_before_triggers, :indexes
      def indexes(table, stream)
        indexes_before_triggers(table, stream)
        triggers(table, stream)
      end

  end
end
