module ActiveRecord
  module ConnectionAdapters
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
        @id                 = id
        @table              = table
        self.binary_type    = binary_type
        self.name           = (name || triggerized_name)
        self.procedure_name = (procedure_name || name || triggerized_name)
      end

      # that's to_r(uby)d(efinition)l(anguage)
      def to_rdl() 
        "  add_trigger #{table.to_sql_name}" <<
        ", [" + events.join(", ") + "]" <<
        (      before? ? ", :before => true"  : "") <<
        (         row? ? ", :row => true"     : "") <<
        (!triggerized? ? ", :name => #{Inflector.symbolize(name)}" : "") <<
        (!triggerized?(procedure_name) ? ", :function => #{Inflector.symbolize(procedure_name)}" : "")
      end

      def binary_type=(*types)
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
  end
end
