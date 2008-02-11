gem 'activesupport'
gem 'activerecord'
require 'active_record'
require "active_support/inflector"

DEBUG = false

module Inflector
  def triggerize(table_name, events=[], before=false)
    events.join(" or ").gsub(":", "").tr(" ", "_").downcase + "_" + (before ? "before_" : "after_") + table_name.to_s + "_trigger"
  end

  def symbolize(val)
    return "'#{val}'" if val =~ /-/
    ":#{val}"
  end
end

module SqlFormat
  def to_sql_name
    '"' + self.to_s + '"'
  end
end

class Symbol
  include SqlFormat
end

class String
  include SqlFormat
end

module SchemaProcs
  @@_cascade_or_restrict = Proc.new {|which| which     ? 'CASCADE' : 'RESTRICT' }
  @@_strict_or_null      = Proc.new {|strict| strict   ? 'STRICT' : 'CALLED ON NULL INPUT' }
  @@_definer_or_invoker  = Proc.new {|definer| definer ? 'DEFINER' : 'INVOKER' }
  @@_behavior            = Proc.new {|volatile| %w{immutable stable volatile}.grep(/^#{volatile[0,1]}.+/).to_s }

  def cascade_or_restrict(cascade=false)
    @@_cascade_or_restrict.call(cascade)
  end
  def strict_or_null(is_strict=false)
    @@_strict_or_null.call(is_strict)
  end
  def definer_or_invoker(definer=false)
    @@_definer_or_invoker.call(definer)
  end
  def behavior(volatile='v')
    @@_behavior.call(volatile)
  end
end

# RailsPgProcs
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

  module ConnectionAdapters
    class ProcedureDefinition < Struct.new(:id, :name)
    end

    class ViewDefinition
      include SchemaProcs
      attr_accessor :id, :name, :columns, :view_body
      def initialize(id, name, columns=[], &block)
        puts "id #{id.inspect} name #{name.inspect} columns #{columns.inspect}" if DEBUG
        @id            = id
        self.name      = name
        self.columns   = columns
        self.view_body = block
        puts "id #{self.id.inspect} name #{self.name.inspect} columns #{self.columns.inspect} view_body #{self.view_body.inspect}" if DEBUG
      end

      def to_rdl
        "  create_view(#{Inflector.symbolize(name)}) { $#{name}_body$\n    #{view_body.call}\n  $#{name}_body$ }"
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

    class TriggerDefinition
      CLEAN  = 0b0
      ROW    = 0b00001
      BEFORE = 0b00010
      INSERT = 0b00100
      DELETE = 0b01000
      UPDATE = 0b10000

      attr_accessor :id, :table, :name, :procedure_name
      attr_reader :binary_type
      def initialize(id, table, name=nil, binary_type=[], procedure_name=nil)
        puts "id #{id.inspect} table #{table.inspect} name #{name.inspect} binary_type #{binary_type.inspect} procedure_name #{procedure_name.inspect}" if DEBUG
        @id                 = id
        @table              = table
        self.binary_type    = binary_type
        self.name           = (name || triggerized_name)
        self.procedure_name = (procedure_name || name || triggerized_name)
        puts "id #{self.id.inspect} table #{self.table.inspect} name #{self.name.inspect} binary_type #{self.binary_type.inspect} procedure_name #{self.procedure_name.inspect}" if DEBUG
      end

      # that's to_r(uby)d(efinition)l(anguage)
      def to_rdl() 
        "  add_trigger(#{Inflector.symbolize(table)}" <<
        ", [" + events.join(", ") + "]" <<
        (      before? ? ", :before => true"  : "") <<
        (         row? ? ", :row => true"     : "") <<
        (!triggerized? ? ", :name => #{Inflector.symbolize(name)}" : "") <<
        (!triggerized?(procedure_name) ? ", :function => #{Inflector.symbolize(procedure_name)}" : "") <<
        ")"
      end

      def binary_type=(*types)
#        print "types #{types.inspect} types[0] #{types[0].inspect} " if DEBUG
        case types[0]
          when Fixnum, Array
            @binary_type = bin_typ(types[0])
          else
            @binary_type = bin_typ(types)
        end
      end

#     CREATE TRIGGER name { BEFORE | AFTER } { event [ OR ... ] }
#     ON table [ FOR [ EACH ] { ROW | STATEMENT } ]
#     EXECUTE PROCEDURE funcname ( arguments )
			def to_sql_create()
				result = "CREATE TRIGGER "          << 
				name.to_sql_name                  << 
				(before? ? " BEFORE" : " AFTER")  <<
				" "                               << 
				(
					events.collect {|event| 
						event.to_s.upcase.gsub(/^:/, '') }.join(" OR ")
				) <<
				" ON "                            << 
				table.to_sql_name                 << 
				" FOR EACH "                      << 
				(row? ? "ROW" : "STATEMENT")      << 
				" EXECUTE PROCEDURE "             << 
				procedure_name.to_sql_name        << 
				"();"
				result
		end

    def triggerized?(nam=nil)
      nam ||= self.name
      triggerized_name == nam
    end

	  def before?
	    calc(BEFORE)
	  end

	  def row?
	    calc(ROW)
		end

      private

      def triggerized_name
        Inflector.triggerize(table, events, calc(BEFORE))
      end

      def events
        events = []
        events.push(":insert") if calc(INSERT)
        events.push(":update") if calc(UPDATE)
        events.push(":delete") if calc(DELETE)
        events
      end

      def calc(bin)
        eval(sprintf("0b%0.8b", self.binary_type())) & bin > 0
      end

      def bin_typ(typs)
#        puts "typs #{typs.inspect} typs.class #{typs.class}" if DEBUG
        case typs
          when Fixnum
            return typs
          when Symbol
            return bin_typ(typs.to_s)
          when String
            return typs.to_i if typs =~ /^\d+$/
            return self.class.const_get(typs.upcase.to_sym)
          when Array
            ctype = 0
            typs.each {|typ| 
              ctype += bin_typ(typ)
            }
        end
        ctype
      end

      # end private
    end
    class TypeDefinition < Struct.new(:id, :name, :columns); end

    # TODO -- Add Aggregates ability
    class PostgreSQLAdapter < AbstractAdapter
      include SchemaProcs

      def procedures(lang=nil)
        query <<-end_sql
          SELECT P.oid, proname, pronamespace, proowner, lanname, proisagg, prosecdef, proisstrict, proretset, provolatile, pronargs, prorettype, proargtypes, proargnames, prosrc, probin, proacl
            FROM pg_proc P
            JOIN pg_language L ON (P.prolang = L.oid)
            JOIN pg_namespace N ON (P.pronamespace = N.oid)
           WHERE N.nspname = 'public'
             AND (proisagg = 'f')
            #{'AND (lanname ' + lang + ')'unless lang.nil?}
        end_sql
      end

      def triggers(table_name)
        query(<<-end_sql).collect {|row| TriggerDefinition.new(*row) }
          SELECT T.oid, C.relname, T.tgname, T.tgtype, P.proname
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
      
      def drop_type(name, cascade=false)
        execute "DROP TYPE #{name} #{cascade_or_restrict(cascade)}"
      end

      def create_view(name, columns=[], options={}, &block)
        view = ViewDefinition.new(0, name, columns) { yield } 
        execute view.to_sql(:create, options)
			end

      def drop_view(name, options={})
        view = ViewDefinition.new(0, name)
        execute view.to_sql(:drop, options)
      end

#     Add a trigger to a table
      def add_trigger(table, events, options={})
        events += [:row]    if options.delete(:row)
        events += [:before] if options.delete(:before)
        trigger = TriggerDefinition.new(0, table, options[:name], events, options[:function])
        execute trigger.to_sql_create
      end

#      DROP TRIGGER name ON table [ CASCADE | RESTRICT ]
      def remove_trigger(table, name, options={})
        options[:name] = name
        execute "DROP TRIGGER #{trigger_name(table, [], options).to_sql_name} ON #{table} #{cascade_or_restrict(options[:deep])};"
      end

#      Create a stored procedure
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
          raise StatementInvalid.new("Missing function source")
        end
      end

#      DROP FUNCTION name ( [ type [, ...] ] ) [ CASCADE | RESTRICT ]
#     default RESTRICT
      def drop_proc(name, columns=[], cascade=false)
        execute "DROP FUNCTION #{name.to_sql_name}(#{columns.collect {|column| column}.join(", ")}) #{cascade_or_restrict(cascade)};"
      end

      private
        def trigger_name(table, events=[], options={})
          options[:name] || Inflector.triggerize(table, events, options[:before])
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
        def delim(name, options)
          options[:delim] || "$#{Inflector.underscore(name)}_body$"
        end
          
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
            body = "#{delim(name, options)}\n#{yield}\n#{delim(name, options)}"
          elsif options[:resource]
            options[:resource] += [name] if options[:resource].size == 1
            body = options[:resource].collect {|res| "'#{res}'" }.join(", ")
          else
            raise StatementInvalid.new("Missing function source")
          end
          
          result = "
		  CREATE OR REPLACE FUNCTION #{name.to_sql_name}(#{columns.collect{|column| column}.join(", ")}) #{returns} AS
			#{body}
			LANGUAGE #{lang}
			#{ behavior(options[:behavior] || 'v').upcase }
			#{ strict_or_null(options[:strict]) }
			EXTERNAL SECURITY #{ definer_or_invoker(options[:definer]) }
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
