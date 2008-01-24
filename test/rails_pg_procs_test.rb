require 'test/unit'
require 'stringio'
require 'runit/assert'
require File.dirname(__FILE__) + '/../lib/rails_pg_procs'
require File.dirname(__FILE__) + '/connection'

class String
  def to_regex
    Regexp.new(self.tr(' ', "\\\s").gsub(/([\(\)\[\]\{\}\.\\\$])/) {|s| '\\' + s })
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
      t.column :when, :datetime
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
    assert_equal 1<<0, ActiveRecord::SchemaDumper::ROW
    assert_equal 1<<1, ActiveRecord::SchemaDumper::BEFORE
    assert_equal 1<<2, ActiveRecord::SchemaDumper::INSERT
    assert_equal 1<<3, ActiveRecord::SchemaDumper::DELETE
    assert_equal 1<<4, ActiveRecord::SchemaDumper::UPDATE
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
        assert_equal "insert_after_test_table_trigger", received.last[1]
      }
    }
    assert_equal procedures_count, @connection.procedures().result.size
  end

  def test_functional_dump
    @connection.create_type(:qualitysmith_user, [:name, :varchar], {:address => "varchar(20)"}, [:zip, "varchar(5)"], [:phone, "numeric(10,0)"])
    @connection.create_proc("name-with-hyphen", [], :return => :trigger) { "  BEGIN\n--Something else goes here\nEND;\n" }
    @connection.create_proc(:update_trade_materials_statuses_logf, [], :return => :trigger) { "  BEGIN\n--Something else goes here\nEND;\n" }
    @connection.create_proc(:levenshtein, [], :return => :trigger, :resource => ['$libdir/fuzzystrmatch'], :lang => "c")
    @connection.add_trigger(:test_table, [:insert, :update], :row => true, :name => :update_trade_materials_statuses_logt, :function => :update_trade_materials_statuses_logf)
    @connection.add_trigger(:test_table, [:insert, :update], :function => :levenshtein)

    stream = StringIO.new
    ActiveRecord::SchemaDumper.ignore_tables = []
    ActiveRecord::SchemaDumper.dump(ActiveRecord::Base.connection, stream)
    received_sql = stream.string

    f = File.open("out.rb", "w")
    f.write received_sql
    f.close
    assert_match('create_type(:qualitysmith_user, [[:name, "character varying"], [:address, "character varying(20)"], [:zip, "character varying(5)"], [:phone, "numeric(10,0)"]])'.to_regex, received_sql)
    assert_match("create_proc('name-with-hyphen'".to_regex, received_sql)
    assert_match('create_proc(:update_trade_materials_statuses_logf'.to_regex, received_sql)
    assert_match('create_proc(:levenshtein, []'.to_regex, received_sql)
    assert_match('add_trigger(:test_table, [:insert, :update], :name => :insert_or_update_after_test_table_trigger, :function => :levenshtein)'.to_regex, received_sql)
    assert_match('add_trigger(:test_table, [:insert, :update], :row => true, :name => :update_trade_materials_statuses_logt, :function => :update_trade_materials_statuses_logf)'.to_regex, received_sql)

    @connection.remove_trigger(:test_table, :update_trade_materials_statuses_logt)
    @connection.remove_trigger(:test_table, :insert_or_update_after_test_table_trigger)
    @connection.drop_proc(:update_trade_materials_statuses_logf)
    @connection.drop_proc(:levenshtein)
    @connection.drop_proc('name-with-hyphen')
    @connection.drop_type(:qualitysmith_user)
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
    @connection.create_type(:qualitysmith_user, [:name, "varchar(10)"], {:zip => "numeric(5,0)"}, [:is_customer => :boolean])
    assert_no_exception(NoMethodError) do 
      dumper = ActiveRecord::SchemaDumper.new(@connection)
      stream = StringIO.new
      dumper.send(:types, stream)
      stream.rewind
      received = stream.read
       assert_equal '  create_type(:qualitysmith_user, [[:name, "character varying(10)"], [:zip, "numeric(5,0)"], [:is_customer, :boolean]])', received.chomp
    end
    @connection.drop_type(:qualitysmith_user)

#	with_proc(:test_sql_type_proc_with_table_reference, [:integer], :return => nil){
#	
#	}
  end

  def test_calculations
    dumper = ActiveRecord::SchemaDumper.new(@connection)
    assert dumper.send("calc", 0b00010101, ActiveRecord::SchemaDumper::INSERT)
    assert dumper.send("calc", 0b00010101, ActiveRecord::SchemaDumper::UPDATE)
    assert dumper.send("calc", 0b00010101, ActiveRecord::SchemaDumper::ROW)
    assert dumper.send("calc", 0b00001010, ActiveRecord::SchemaDumper::DELETE)
    assert dumper.send("calc", 0b00001010, ActiveRecord::SchemaDumper::BEFORE)
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
      assert_match(re, @connection.send("get_proc_query", "logf", [], :return => nil, :user => 'definer', :strict => true, :behavior => 'immutable') { @query_body })
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
  end

  def test_add_trigger
    assert_equal("CREATE TRIGGER insert_or_update_after_trade_materials_trigger AFTER INSERT OR UPDATE ON trade_materials FOR EACH STATEMENT EXECUTE PROCEDURE insert_or_update_after_trade_materials_trigger();", \
      @connection.send("get_trigger_query", "trade_materials", [:insert, :update])
    )
    assert_equal("CREATE TRIGGER update_trade_materials_statuses_logt AFTER INSERT OR UPDATE ON trade_materials FOR EACH STATEMENT EXECUTE PROCEDURE update_trade_materials_statuses_logt();", \
      @connection.send("get_trigger_query", "trade_materials", [:insert, :update], :name => "update_trade_materials_statuses_logt")
    )
    assert_equal("CREATE TRIGGER update_trade_materials_statuses_logt BEFORE INSERT OR UPDATE ON trade_materials FOR EACH ROW EXECUTE PROCEDURE update_trade_materials_statuses_logf();", \
      @connection.send("get_trigger_query", "trade_materials", [:insert, :update], :before => true, :row => true, :name => "update_trade_materials_statuses_logt", :function => "update_trade_materials_statuses_logf")
    )

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

  def with_trigger(table, columns=[], options={}, &block)
    @connection.add_trigger(table, columns, options) 
      yield
    @connection.remove_trigger(table, options[:name] || Inflector.triggerize(table, columns, options.has_key?(:before)))
  end
end
