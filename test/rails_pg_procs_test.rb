require 'test/unit'
require 'stringio'
require 'runit/assert'
require "#{File.dirname(__FILE__)}/../lib/rails_pg_procs"
require "#{File.dirname(__FILE__)}/connection"

class String
  def to_regex
    Regexp.new(self.gsub(/([\s\n\t]+)/, '\\1+').gsub(/([\(\)\[\]\{\}\.\\\$])/) {|s| '\\' + s })
  end
end


class ActiveRecord::SchemaDumper; public_class_method :new; end
# we want new public for the tests so we can test each method independantly from the rest

class RailsPgProcsTest < Test::Unit::TestCase
#  include ActiveRecord::ConnectionAdapters::SchemaStatements
  @_use_transactional_fixtures = false

  include RUNIT::Assert
  def setup
    super
    @connection = ActiveRecord::Base.connection
    @connection.create_table(:test_table, :force => true) {|t|
      t.column :name, :text
      t.column :when, :timestamp
    }

    @query_body = "
  BEGIN
        -- do something --
  END;
"
  end

  def teardown
    @connection.drop_table(:test_table)
  end

  def test_constants
    assert_equal 1<<0, ActiveRecord::ConnectionAdapters::TriggerDefinition::ROW
    assert_equal 1<<1, ActiveRecord::ConnectionAdapters::TriggerDefinition::BEFORE
    assert_equal 1<<2, ActiveRecord::ConnectionAdapters::TriggerDefinition::INSERT
    assert_equal 1<<3, ActiveRecord::ConnectionAdapters::TriggerDefinition::DELETE
    assert_equal 1<<4, ActiveRecord::ConnectionAdapters::TriggerDefinition::UPDATE
  end

  def test_methods
    %w(add_trigger remove_trigger create_proc drop_proc procedures triggers types).each {|meth|
      assert_respond_to meth, @connection
    }

    assert !@connection.procedures().nil?, "@connection#procedures returns nil"
    @connection.drop_proc(:insert_after_test_table_trigger) if "insert_after_test_table_trigger" == @connection.procedures.result.last[1]
    procedures_count = @connection.procedures().result.size
    trigger_count = @connection.triggers(:test_table).size
    with_proc(:insert_after_test_table_trigger, [], :return => :trigger) {
      assert_equal 0, trigger_count
      assert_equal procedures_count + 1, @connection.procedures().result.size
      with_trigger(:test_table, [:insert], :row => true) {
        assert !@connection.triggers(:test_table).nil?, "Triggers for table :test_table returns nil"
        received = @connection.triggers(:test_table)
        assert_equal trigger_count + 1, received.size
        assert_equal "insert_after_test_table_trigger", received.last.name
      }
    }
    assert_equal procedures_count, @connection.procedures().result.size
  end

  def test_functional_dump
    @connection.create_proc(:f_commacat, [:text, :text], :return => :text) { "BEGIN
  IF (LENGTH($1) > 0 ) THEN
     RETURN $1 || ', ' || $2;
  ELSE
     RETURN $2;
  END IF;
END;" }
    begin
      @connection.execute "DROP AGGREGATE comma(text);"
      @connection.execute "CREATE AGGREGATE comma(BASETYPE=text, SFUNC=f_commacat, STYPE=text)"
    rescue ActiveRecord::StatementInvalid
    end
    @connection.create_type(:qualitysmith_user, [:name, :varchar], {:address => "varchar(20)"}, [:zip, "varchar(5)"], [:phone, "numeric(10,0)"])
    @connection.create_proc("name-with-hyphen", [], :return => :trigger) { "  BEGIN\n--Something else goes here\nEND;\n" }
    @connection.create_proc(:update_trade_materials_statuses_logf, [], :return => :trigger) { "  BEGIN\n--Something else goes here\nEND;\n" }
    @connection.create_proc(:levenshtein, [], :return => :trigger, :resource => ['$libdir/fuzzystrmatch'], :lang => "c")
    @connection.add_trigger(:test_table, [:insert, :update], :row => true, :name => "update_trade-materials_statuses_logt", :function => :update_trade_materials_statuses_logf)
    @connection.add_trigger(:test_table, [:insert, :update], :function => :levenshtein)
    @connection.create_table(:a_table_defined_after_the_stored_proc, :force => true) {|t|
      t.column :name, :varchar
    }
    @connection.create_proc(:sql_proc_with_table_reference, [:integer], :return => :integer, :lang => "SQL") { "SELECT id FROM a_table_defined_after_the_stored_proc WHERE id = $1;" }

    stream = StringIO.new
    ActiveRecord::SchemaDumper.ignore_tables = []
    ActiveRecord::SchemaDumper.dump(ActiveRecord::Base.connection, stream)
    received_sql = stream.string

    assert_match('create_type(:qualitysmith_user, [:name, "character varying"], [:address, "character varying(20)"], [:zip, "character varying(5)"], [:phone, "numeric(10,0)"])'.to_regex, received_sql)
    assert_match("create_proc('name-with-hyphen'".to_regex, received_sql)
    assert_match('create_proc(:update_trade_materials_statuses_logf'.to_regex, received_sql)
    assert_match('create_proc(:levenshtein, []'.to_regex, received_sql)
    assert_match('add_trigger(:test_table, [:insert, :update], :function => :levenshtein)'.to_regex, received_sql)
    assert_match("add_trigger(:test_table, [:insert, :update], :row => true, :name => 'update_trade-materials_statuses_logt', :function => :update_trade_materials_statuses_logf)".to_regex, received_sql)
    assert_match('create_table "a_table_defined_after_the_stored_proc"'.to_regex, received_sql)
    assert_match("create_proc(:sql_proc_with_table_reference, [:int4], :return => :int4, :lang => 'sql') {\n    <<-sql_proc_with_table_reference_sql\n\nSELECT id FROM a_table_defined_after_the_stored_proc WHERE id = $1;\n    sql_proc_with_table_reference_sql\n  }".to_regex, received_sql.split("\n")[-9..-2].join("\n"))
    assert_match("create_proc(:f_commacat, [:text, :text]".to_regex, received_sql)
    assert_no_match("lang => 'internal'".to_regex, received_sql)
    assert_no_match("create_proc(:comma".to_regex, received_sql)

    @connection.drop_proc(:sql_proc_with_table_reference, [:int4])
    @connection.drop_table(:a_table_defined_after_the_stored_proc)
    @connection.remove_trigger(:test_table, :insert_or_update_after_test_table_trigger)
    @connection.remove_trigger(:test_table, "update_trade-materials_statuses_logt")
    @connection.drop_proc(:levenshtein)
    @connection.drop_proc(:update_trade_materials_statuses_logf)
    @connection.drop_proc('name-with-hyphen')
    @connection.drop_type(:qualitysmith_user)
    begin
      @connection.execute "DROP AGGREGATE comma(text)"
    rescue ActiveRecord::StatementInvalid
    end
    @connection.drop_proc(:f_commacat, [:text, :text])
  end

  def test_sym_to_str
    assert_equal '"abc"', "abc".to_sql_name
    assert_equal '"abc"', "abc".to_sym.to_sql_name
  end

  def test_schema_dumper
    # a simple test
    with_proc(:insert_after_test_table_trigger, [], :return => :trigger) {
      with_trigger(:test_table, [:insert], :row => true) {
        assert_no_exception(NoMethodError) do 
          stream = StringIO.new
          dumper = ActiveRecord::SchemaDumper.new(@connection)
          dumper.send(:triggers, :test_table, stream)
          stream.rewind
          assert_equal "  add_trigger(:test_table, [:insert], :row => true)\n\n", stream.read

          stream = StringIO.new
          dumper.send(:procedures, stream)
          stream.rewind
          assert_match "  create_proc(:insert_after_test_table_trigger, [], :return => :trigger, :lang => 'plpgsql') {\n    <<-insert_after_test_table_trigger_sql\n\n#{@query_body}\n    insert_after_test_table_trigger_sql\n  }\n".to_regex, stream.read
        end
      }
    }

    # a more complicated test
    with_proc(:update_trade_materials_statuses_logf, [], :return => :trigger) {
      with_trigger(:test_table, [:insert, :update], :before => true, :name => :update_trade_materials_statuses_logt, :function => :update_trade_materials_statuses_logf) {
        assert_no_exception(NoMethodError) do 
          stream = StringIO.new
          dumper = ActiveRecord::SchemaDumper.new(@connection)
          dumper.send(:triggers, :test_table, stream)
          stream.rewind
          assert_equal "  add_trigger(:test_table, [:insert, :update], :before => true, :name => :update_trade_materials_statuses_logt, :function => :update_trade_materials_statuses_logf)\n\n", stream.read

          stream = StringIO.new
          dumper.send(:procedures, stream)
          stream.rewind
          received_sql = stream.string
          assert_match "  create_proc(:update_trade_materials_statuses_logf, [], :return => :trigger, :lang => 'plpgsql') {\n    <<-update_trade_materials_statuses_logf_sql\n\n#{@query_body}\n    update_trade_materials_statuses_logf_sql\n  }\n".to_regex, received_sql
        end
      }
    }

    # a more-more complicated test
    with_proc(:levenshtein, [:text, :text], :return => nil, :resource => ['$libdir/fuzzystrmatch'], :strict => true, :behavior => 'immutable', :lang => "C") {
      assert_no_exception(NoMethodError) do 
        dumper = ActiveRecord::SchemaDumper.new(@connection)
        stream = StringIO.new
        dumper.send(:procedures, stream)
        stream.rewind
        received = stream.read
        assert_equal "  create_proc(:levenshtein, [:text, :text], :return => nil, :resource => ['$libdir/fuzzystrmatch', 'levenshtein'], :strict => true, :behavior => 'immutable', :lang => 'c')", received.split("\n")[-1]
      end
    }

    # a type test
#    @connection.drop_type :qualitysmith_user
    @connection.create_type(:qualitysmith_user, [:name, "varchar(10)"], {:zip => "numeric(5,0)"}, [:is_customer => :boolean])
    assert_no_exception(NoMethodError) do 
      dumper = ActiveRecord::SchemaDumper.new(@connection)
      stream = StringIO.new
      dumper.send(:types, stream)
      stream.rewind
      received = stream.read
       assert_equal '  create_type(:qualitysmith_user, [:name, "character varying(10)"], [:zip, "numeric(5,0)"], [:is_customer, :boolean])', received.chomp
    end
    @connection.drop_type(:qualitysmith_user)

    proc_name, columns = "test_sql_type_proc_with_table_reference", [:integer]
    assert_not_equal proc_name, @connection.procedures.result.last[1]
    assert_raise ActiveRecord::StatementInvalid do
      @connection.create_proc(proc_name, columns, :return => nil, :lang => :sql) { 
        <<-sql
          SELECT * FROM a_table_that_doesnt_yet_exist WHERE id = '$1';
        sql
      }
    end
    assert_not_equal proc_name, @connection.procedures.result.last[1]
    @connection.create_table(:a_table_that_doesnt_yet_exist, :force => true) { |t|
      t.column :name, :varchar
    }

    assert_not_equal proc_name, @connection.procedures.result.last[1]
    assert_nothing_raised do
      @connection.create_proc(proc_name, columns, :return => :integer, :lang => :sql, :force => true) { 
        <<-sql
          SELECT id FROM a_table_that_doesnt_yet_exist WHERE id = $1;
        sql
      }
    end
    @connection.drop_table(:a_table_that_doesnt_yet_exist)
    @connection.drop_proc(proc_name, columns)
    assert_not_equal proc_name, @connection.procedures.result.last[1]
  end

  def test_trigger_definition_class
    trig = ActiveRecord::ConnectionAdapters::TriggerDefinition.new(0, 'test_table', 'name', 0b00010101, 'function')
    assert_equal('name',       trig.name)
    assert_equal('test_table', trig.table)
    assert_equal(0b00010101,   trig.binary_type)
    assert_equal('function',   trig.procedure_name)
    trig.binary_type = 0b00011101
    assert_equal(29, trig.binary_type)
    trig.binary_type = :insert, :update
    assert_equal(20, trig.binary_type)
    trig.send("binary_type=", :insert, :delete)
    assert_equal(12, trig.binary_type)
    trig = ActiveRecord::ConnectionAdapters::TriggerDefinition.new(0, 'test_table', nil, [:insert, :delete])
    assert_equal('insert_or_delete_after_test_table_trigger',       trig.name)
    assert_equal('insert_or_delete_after_test_table_trigger',       trig.procedure_name)

    trig.procedure_name = "update_trigger"
    assert_equal('insert_or_delete_after_test_table_trigger',       trig.name)
    assert_equal('update_trigger'                           ,       trig.procedure_name)
    assert trig.triggerized?
    assert !trig.triggerized?(trig.procedure_name)
  end 

  def test_calculations
    trig = ActiveRecord::ConnectionAdapters::TriggerDefinition.new(1, 'test_table', 'name', 0b00010101, 'function')
    assert(trig.send("calc", ActiveRecord::ConnectionAdapters::TriggerDefinition::INSERT))
    assert(trig.send("calc", ActiveRecord::ConnectionAdapters::TriggerDefinition::UPDATE))
    assert(trig.send("calc", ActiveRecord::ConnectionAdapters::TriggerDefinition::ROW))
    trig.binary_type = 0b00001010
    assert_equal(10, trig.binary_type)
    assert(trig.send("calc", ActiveRecord::ConnectionAdapters::TriggerDefinition::DELETE))
    assert(trig.send("calc", ActiveRecord::ConnectionAdapters::TriggerDefinition::BEFORE))
    trig.send("binary_type=", :insert, :row, :before)
    assert_equal(7, trig.binary_type)
    assert(trig.send("calc", ActiveRecord::ConnectionAdapters::TriggerDefinition::INSERT))
    assert(trig.send("calc", ActiveRecord::ConnectionAdapters::TriggerDefinition::BEFORE))
    assert(trig.send("calc", ActiveRecord::ConnectionAdapters::TriggerDefinition::ROW))
    trig.send("binary_type=", :insert, :update)
    trig.binary_type = 0b00010100
    assert_equal(20, trig.binary_type)
    assert(trig.send("calc", ActiveRecord::ConnectionAdapters::TriggerDefinition::INSERT))
    assert(trig.send("calc", ActiveRecord::ConnectionAdapters::TriggerDefinition::UPDATE))
  end

  def test_create_type()
    assert_exception(ActiveRecord::StatementInvalid, "Missing columns") {
      @connection.create_type("user", {})
    }
    assert_exception(ActiveRecord::StatementInvalid, "Missing columns") {
      @connection.create_type("user", [])
    }

    [
      'CREATE TYPE "user"',
      '"name" varchar(10)',
      '"zip" numeric(5,0)'
    ].each {|re|
      assert_match re.to_regex, @connection.send("get_type_query", "user", [:name, "varchar(10)"], {:zip => "numeric(5,0)"}, [:is_customer => :boolean])
    }

#    @connection.drop_type :qualitysmith_user
    assert_nothing_raised {
      @connection.create_type(:qualitysmith_user, [:name, "varchar(10)"], {:zip => "numeric(5,0)"}, [:is_customer => :boolean])
    }
    count = @connection.select_value("select count(*) from pg_type where typname = 'qualitysmith_user'", "count")
    assert_equal("1", count)
    assert_nothing_raised {
      @connection.drop_type(:qualitysmith_user)
    }
    count = @connection.select_value("select count(*) from pg_type where typname = 'qualitysmith_user'", "count")
    assert_equal("0", count)
  end

  def test_create_proc()
    [
      /logf()/,
      /\'resource\/file\',\s\'method\'/,
      /LANGUAGE C/
    ].each {|re| 
      assert_match(re, @connection.send("get_proc_query", "logf", [], :return => nil, :resource => ['resource/file', 'method'], :lang => "C"))
    }

    [
      /logf()/,
      /\'resource\/file\',\s\'logf\'/,
      /LANGUAGE C/
    ].each {|re| 
      assert_match(re, @connection.send("get_proc_query", "logf", [], :return => nil, :resource => ['resource/file'], :lang => "C"))
    }

    [
      /CREATE OR REPLACE FUNCTION "logf"()/,
      /RETURNS VOID/,
      /\$logf_body\$/,
      @query_body.to_regex,
      /LANGUAGE plpgsql/,
      /IMMUTABLE/,
      /STRICT/,
      /EXTERNAL SECURITY DEFINER/
    ].each {|re| 
      assert_match(re, @connection.send("get_proc_query", "logf", [], :return => nil, :definer => true, :strict => true, :behavior => 'immutable') { @query_body })
    }

    [
      /update_trade_materials_statuses_logf()/,
      /RETURNS VOID/,
      /\$update_trade_materials_statuses_logf_body\$/,
      @query_body.to_regex,
      /LANGUAGE SQL/,
    ].each {|re| 
      assert_match(re, @connection.send("get_proc_query", "update_trade_materials_statuses_logf", [], :return => nil, :lang => :SQL) { @query_body })
    }

    [
      /update_trade_materials_statuses_logf()/,
      /RETURNS trigger/,
      /\$update_trade_materials_statuses_logf_body\$/,
      @query_body.to_regex,
      /LANGUAGE plpgsql/,
      /VOLATILE/,
      /CALLED ON NULL INPUT/,
      /EXTERNAL SECURITY INVOKER/
    ].each {|re| 
      assert_match(re, @connection.send("get_proc_query", "update_trade_materials_statuses_logf", [], :return => "trigger") { @query_body })
    }

    assert_exception(ActiveRecord::StatementInvalid, "Missing block or library file.") {
      @connection.create_proc("update_trade_materials_statuses_logf", [], :return => "trigger")
    }

    assert_nothing_raised {
      @connection.create_proc(:update_trade_materials_statuses_logf, [], :return => :trigger) {
        @query_body
      }
    }
    count = @connection.select_value("select count(*) from pg_proc where proname = 'update_trade_materials_statuses_logf'", "count")
    assert_equal("1", count)
    assert_nothing_raised {
      @connection.drop_proc("update_trade_materials_statuses_logf", [])
    }
    count = @connection.select_value("select count(*) from pg_proc where proname = 'update_trade_materials_statuses_logf'", "count")
    assert_equal("0", count)

    assert_equal('CASCADE',  @connection.send("cascade_or_restrict", true))
    assert_equal('RESTRICT', @connection.send("cascade_or_restrict"))
    assert_equal('RESTRICT', @connection.send("cascade_or_restrict", false))
  end

  def test_view_definition_class
    view = ActiveRecord::ConnectionAdapters::ViewDefinition.new(0, :trade_materials_view) { "SELECT 'aview'" }
    [
      /CREATE OR REPLACE VIEW/,
      /\"trade_materials_view\"/,
      /\n[\s\t]+AS SELECT 'aview'/,
    ].each {|re|
      assert_match(re, view.to_sql)
    }
    assert_equal('DROP VIEW "trade_materials_view" RESTRICT', view.to_sql(:drop))

    [
      /create_view\(:trade_materials_view\)/,
      /SELECT 'aview'/,
      /\$trade_materials_view_body\$/
    ].each {|re|
      assert_match(re, view.to_rdl())
    }

    count = @connection.select_value("SELECT count(*) FROM pg_class WHERE relname = 'trade_materials_view' AND relkind = 'v'", "count")
    assert_equal("0", count)
    assert_nothing_raised {
      @connection.create_view("trade_materials_view") { "SELECT 'aview'" }
    }
    count = @connection.select_value("SELECT count(*) FROM pg_class WHERE relname = 'trade_materials_view' AND relkind = 'v'", "count")
    assert_equal("1", count)
    assert_nothing_raised {
      @connection.drop_view("trade_materials_view")
    }
    count = @connection.select_value("SELECT count(*) FROM pg_class WHERE relname = 'trade_materials_view' AND relkind = 'v'", "count")
    assert_equal("0", count)
  end

  def test_add_trigger
    trig = ActiveRecord::ConnectionAdapters::TriggerDefinition.new(0, "trade_materials", nil, [:insert, :update])
    assert_equal('CREATE TRIGGER "insert_or_update_after_trade_materials_trigger" AFTER INSERT OR UPDATE ON "trade_materials" FOR EACH STATEMENT EXECUTE PROCEDURE "insert_or_update_after_trade_materials_trigger"();', trig.to_sql_create)
    assert_equal('  add_trigger(:trade_materials, [:insert, :update])', trig.to_rdl())

    trig = ActiveRecord::ConnectionAdapters::TriggerDefinition.new(0, "trade_materials", "update_trade_materials_statuses_logt", [:insert, :update])
    assert_equal('CREATE TRIGGER "update_trade_materials_statuses_logt" AFTER INSERT OR UPDATE ON "trade_materials" FOR EACH STATEMENT EXECUTE PROCEDURE "update_trade_materials_statuses_logt"();', trig.to_sql_create)
    assert_equal('  add_trigger(:trade_materials, [:insert, :update], :name => :update_trade_materials_statuses_logt, :function => :update_trade_materials_statuses_logt)', trig.to_rdl)

    trig = ActiveRecord::ConnectionAdapters::TriggerDefinition.new(0, "trade_materials", "update_trade_materials_statuses_logt", [:insert, :update, :before, :row], "update_trade_materials_statuses_logf")
    assert_equal('CREATE TRIGGER "update_trade_materials_statuses_logt" BEFORE INSERT OR UPDATE ON "trade_materials" FOR EACH ROW EXECUTE PROCEDURE "update_trade_materials_statuses_logf"();', trig.to_sql_create)
    assert_equal('  add_trigger(:trade_materials, [:insert, :update], :before => true, :row => true, :name => :update_trade_materials_statuses_logt, :function => :update_trade_materials_statuses_logf)', trig.to_rdl)

    trig = ActiveRecord::ConnectionAdapters::TriggerDefinition.new(0, "trade_materials", nil, [:insert, :update], "update_trade_materials_statuses_logf")
    assert_equal('CREATE TRIGGER "insert_or_update_after_trade_materials_trigger" AFTER INSERT OR UPDATE ON "trade_materials" FOR EACH STATEMENT EXECUTE PROCEDURE "update_trade_materials_statuses_logf"();', trig.to_sql_create)
    assert_equal('  add_trigger(:trade_materials, [:insert, :update], :function => :update_trade_materials_statuses_logf)', trig.to_rdl)

    @connection.create_proc("update_trade_materials_statuses_logf", [], :return => "trigger") { @query_body }
    assert_nothing_raised {
      @connection.add_trigger("test_table", [:insert, :update], :row => true, :name => "update_trade_materials_statuses_logt", :function => "update_trade_materials_statuses_logf") 
    }
    count = @connection.select_value("SELECT count(*) FROM pg_trigger WHERE tgname = 'update_trade_materials_statuses_logt' and tgrelid = (SELECT oid FROM pg_class WHERE relname = 'test_table')", "count")
    assert_equal("1", count)
    assert_nothing_raised {
      @connection.remove_trigger("test_table", "update_trade_materials_statuses_logt")
    }
    count = @connection.select_value("SELECT count(*) FROM pg_trigger WHERE tgname = 'update_trade_materials_statuses_logt' and tgrelid = (SELECT oid FROM pg_class WHERE relname = 'test_table')", "count")
    assert_equal("0", count)
    @connection.drop_proc("update_trade_materials_statuses_logf", [])
  end

  def with_proc(name, columns=[], options={}, &block)
    assert_not_equal name, @connection.procedures.result.last[1]
    if options[:resource]
      @connection.create_proc(name, columns, options)
    else
      @connection.create_proc(name, columns, options) { @query_body }
    end
    assert_equal name.to_s, @connection.procedures.result.last[1]
      yield
    @connection.drop_proc(name, columns)
    assert_not_equal name.to_s, @connection.procedures.result.last[1]
  end

  def with_trigger(table, events=[], options={}, &block)
    @connection.add_trigger(table, events, options) 
      yield
    @connection.remove_trigger(table, options[:name] || Inflector.triggerize(table, events, options.has_key?(:before)))
  end
end
