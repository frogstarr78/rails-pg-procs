gem 'activesupport'
gem 'activerecord'
require 'active_record'
require "active_support/inflector"

module Inflector
  def triggerize(table_name, events=[], before=false)
    events.join(" or ").sub(":", "").tr(" ", "_").downcase + "_" + (before ? "before_" : "after_") + table_name.to_s + "_trigger"
  end

  def symbolize(val)
    return "'#{val}'" if val =~ /-/
    ":#{val}"
  end
end

# RailsPgProcs
module ActiveRecord
  # This class is used to dump the database schema for some connection to some
  # output format (i.e., ActiveRecord::Schema).
  class SchemaDumper
    ROW    = 0b00001
    BEFORE = 0b00010
    INSERT = 0b00100
    DELETE = 0b01000
    UPDATE = 0b10000

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

      def procedures(stream, conditions=nil)
        @connection.procedures(conditions).each { |proc_row|
          oid, name, namespace, owner, lang, is_agg, sec_def, is_strict, ret_set, volatile, nargs, ret_type, arg_types, arg_names, src, bin, acl = proc_row
          is_agg    = is_agg    == 't'
          is_strict = is_strict == 't'
          ret_set   = ret_set   == 't'
          volatile  = %w{immutable stable}.grep(/^#{volatile}.+/).to_s unless volatile == 'v'
          arg_names ||= ''
          args      = get_type(arg_types.split(" "))#.zip(arg_names.split(" "))

          stream.print "  create_proc(#{Inflector.symbolize(name)}, [#{args}], :return => #{get_type(ret_type)}"
          stream.print ", :resource => ['#{bin}', '#{src}']" unless bin == '-'
          stream.print ", :set => true" if ret_set
          stream.print ", :strict => true" if is_strict
          stream.print ", :behavior => '#{volatile}'" unless volatile == 'v'
          stream.print ", :lang => '#{lang}')"
          stream.print " {\n    <<-#{Inflector.underscore(name)}_sql\n#{src.chomp}\n    #{Inflector.underscore(name)}_sql\n  }" if bin == '-'
          stream.print "\n"
        }
      end
      def triggers(table_name, stream)
        triggers = @connection.triggers(table_name)
        unless triggers.empty?
          triggers.each {|trigger|
            stream.print "  add_trigger(#{Inflector.symbolize(table_name)}" 
           events = []
            events.push(":insert") if calc(trigger.type, INSERT)
            events.push(":update") if calc(trigger.type, UPDATE)
            events.push(":delete") if calc(trigger.type, DELETE)

            stream.print ", [" + events.join(", ") + "]"
            stream.print ", :before => true" if calc(trigger.type, BEFORE)
            stream.print ", :row => true" if calc(trigger.type, ROW)
            stream.print ", :name => #{Inflector.symbolize(trigger.name)}" if Inflector.triggerize(table_name, events, calc(trigger.type, BEFORE)) != trigger.name
            stream.print ", :function => #{Inflector.symbolize(trigger.procedure_name)}" if Inflector.triggerize(table_name, events, calc(trigger.type, BEFORE)) != trigger.name
            stream.puts ")"
          }
        end
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

      def calc(int, bin)
        eval(sprintf("0b%0.8b", int)) & bin > 0
      end
  end

  module ConnectionAdapters

    class TriggerDefinition < Struct.new(:id, :name, :type, :procedure_name); end
    class TypeDefinition < Struct.new(:id, :name, :columns); end

    class PostgreSQLAdapter < AbstractAdapter
      cattr_accessor :first_proc_oid
#      @@first_proc_oid = 10634
      @@first_proc_oid = "(SELECT (MAX(pg_proc.oid::int)-MIN(pg_proc.oid::int))/2 FROM pg_proc)"
      def procedures(lang=nil)
        query <<-end_sql
          SELECT P.oid, proname, pronamespace, proowner, lanname, proisagg, prosecdef, proisstrict, proretset, provolatile, pronargs, prorettype, proargtypes, proargnames, prosrc, probin, proacl
            FROM pg_proc P
            JOIN pg_language L ON (P.prolang = L.oid)
           WHERE P.oid > #{self.class.first_proc_oid}
            #{'AND (lanname ' + lang + ')'unless lang.nil?}
        end_sql
      end

      def triggers(table_name)
        query(<<-end_sql).collect {|row| TriggerDefinition.new(*row) }
          SELECT T.oid, T.tgname, T.tgtype, P.proname
            FROM pg_trigger T
            JOIN pg_class   C ON (T.tgrelid = C.OID AND C.relname = '#{table_name}' AND T.tgisconstraint = 'f')
            JOIN pg_proc    P ON (T.tgfoid = P.OID)
        end_sql
      end

      def types
        result = query(<<-end_sql)
          SELECT T.oid, T.typname, A.attname, format_type(A.atttypid, A.atttypmod) AS type
            FROM pg_type      T
            JOIN pg_class     C ON (T.typrelid = C.oid)
            JOIN pg_attribute A ON (A.attrelid = C.oid AND C.relkind = 'c')
        end_sql

        type_id = nil
        types = []
        result.each { |row|
          if type_id != row[0]
            types << TypeDefinition.new(row[0], row[1], [])
            type_id = row[0]
          end

          types.last.columns << [row[2], row[3]]
        }

        types
      end

      def create_type(name, *columns)
        drop_type(name) if types.find {|typ| typ.name == name.to_s }
        execute get_type_query(name, *columns)
      end
      
      def drop_type(name, drop_dependants="RESTRICT")
        execute "DROP TYPE #{name} #{drop_dependants.to_s.upcase}"
      end

      def add_trigger(table, events, options={})
        execute get_trigger_query(table, events, options)
      end

#      DROP TRIGGER name ON table [ CASCADE | RESTRICT ]
      def remove_trigger(table, name, options={})
        execute "DROP TRIGGER #{name} ON #{table} #{options[:deep] || 'RESTRICT'};"
      end

      def create_proc(name, columns=[], options={}, &block)
        if select_value("SELECT count(oid) FROM pg_language WHERE lanname = 'plpgsql' ","count").to_i == 0
          execute("CREATE TRUSTED PROCEDURAL LANGUAGE plpgsql HANDLER plpgsql_call_handler")
        end

        if options[:force]
          drop_proc(name, columns) rescue nil
        end

        if block_given?
          execute get_proc_query(name, columns, options) { yield }
        elsif options[:resource]
          execute get_proc_query(name, columns, options)
        else
          raise StatementInvalid.new
        end
      end

#      DROP FUNCTION name ( [ type [, ...] ] ) [ CASCADE | RESTRICT ]
#     default RESTRICT
      def drop_proc(name, columns=[], options={})
        execute "DROP FUNCTION \"#{name}\"(#{columns.collect {|column| column}.join(", ")}) #{options[:deep] || 'RESTRICT'};"
      end

      private 
#       CREATE TRIGGER name { BEFORE | AFTER } { event [ OR ... ] }
#       ON table [ FOR [ EACH ] { ROW | STATEMENT } ]
#       EXECUTE PROCEDURE funcname ( arguments )
        def get_trigger_query(table, events, options={})
          event_str = events.collect {|event| event.to_s.upcase }.join(" OR ")
          trigger_name = options[:name] || Inflector.triggerize(table, events, options[:before])
          func_name = options[:function] || trigger_name
          result = "CREATE TRIGGER #{trigger_name} #{(options[:before] ? "BEFORE" : "AFTER")} #{event_str} ON #{table} FOR EACH #{(options[:row] ? "ROW" : "STATEMENT")} EXECUTE PROCEDURE #{func_name}();"
        end

#       Helper function that builds the sql query used to create a stored procedure.
#       Mostly this is here so we can unit test the generated sql.
#       Either an option[:resource] or block must be defined for this method. 
#       Otherwise an ActiveRecord::StatementInvalid exception is raised.
#       Defaults are: 
#          RETURNS (no default -- which is cheap since that means you have to call this method w/ the options Hash) TODO: fix this
#          LANGUAGE = plpgsql (The plugin will add this if you don't have it added already)
#          behavior = VOLATILE (Don't specify IMMUTABLE or STABLE and this will be added for you)
#          strict = CALLED ON NULL INPUT (Otherwise STRICT, According to the 8.0 manual STRICT and RETURNS NULL ON NULL INPUT (RNONI)
#		     behave the same so I didn't make a case for RNONI)
#          user = INVOKER
          
        
#       From PostgreSQL
##      CREATE [ OR REPLACE ] FUNCTION
##          name ( [ [ argmode ] [ argname ] argtype [, ...] ] )
##          [ RETURNS rettype ]
##        { LANGUAGE langname
##          | IMMUTABLE | STABLE | VOLATILE
##          | CALLED ON NULL INPUT | RETURNS NULL ON NULL INPUT | STRICT
#          | [ EXTERNAL ] SECURITY INVOKER | [ EXTERNAL ] SECURITY DEFINER
##          | AS 'definition'
#          | AS 'obj_file', 'link_symbol'
#        } ...
#          [ WITH ( isStrict &| isCacheable ) ]
		# TODO Implement [ [ argmode ] [ argname ] argtype ]
        def get_proc_query(name, columns=[], options={}, &block)
          returns = ''
          if options.has_key?(:return)
            returns = "RETURNS#{' SETOF' if options[:set]} #{options[:return] || 'VOID'}"
          end
          lang = options[:lang] || "plpgsql"

          if block_given?
            body = "$#{Inflector.underscore(name)}_body$
#{yield}
$#{Inflector.underscore(name)}_body$"
          elsif options[:resource]
            options[:resource] += [name] if options[:resource].size == 1
            body = options[:resource].collect {|res| "'#{res}'" }.join(", ")
          else
            raise StatementInvalid.new and return
          end

          result = "
		  CREATE OR REPLACE FUNCTION \"#{name}\"(#{columns.collect{|column| column}.join(", ")}) #{returns} AS
			#{body}
			LANGUAGE #{lang}
			#{ (options[:behavior] || 'VOLATILE').upcase }
			#{ options[:strict] ? 'STRICT' : 'CALLED ON NULL INPUT'}
			EXTERNAL SECURITY #{ options[:user] == 'definer' ? 'DEFINER' : 'INVOKER' }
		  "
        end

        def get_type_query(name, *columns)
          raise StatementInvalid.new if columns.empty?
          "CREATE TYPE #{quote_column_name(name)} AS (
            #{columns.collect{|column,type|
			  if column.is_a?(Hash)
				column.collect { |column, type| "#{quote_column_name(column)} #{type}" }
			  else
			  "#{quote_column_name(column)} #{type}"
			  end
            }.join(",\n")}
          )"
        end
    end
  end
end
