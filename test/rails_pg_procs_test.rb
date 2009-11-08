require 'test_helper'


class RailsPgProcsTest < Test::Unit::TestCase
#  include ActiveRecord::ConnectionAdapters::SchemaStatements

  def test_complicated_schema_dumper
    with_proc(:update_trade_materials_statuses_logf, [], :return => :trigger) {
      with_trigger(:test_table, [:insert, :update], :before => true, :name => :update_trade_materials_statuses_logt, :function => :update_trade_materials_statuses_logf) {
        assert_no_exception(NoMethodError) do 
          stream = StringIO.new
          dumper = ActiveRecord::SchemaDumper.new(@connection)
          dumper.send(:triggers, :test_table, stream)
          stream.rewind
          assert_match /add_trigger "test_table", [:insert, :update], :before => true, :name => :update_trade_materials_statuses_logt, :function => :update_trade_materials_statuses_logf/, stream.read

          stream = StringIO.new
          dumper.send(:procedures, stream)
          stream.rewind
          received_sql = stream.string
          assert_match "  create_proc(\"update_trade_materials_statuses_logf\", [], :return => :trigger, :lang => 'plpgsql') {\n    <<-update_trade_materials_statuses_logf_sql\n\n#{@query_body}\n    update_trade_materials_statuses_logf_sql\n  }".to_regex, received_sql
        end
      }
    }
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

    @connection.create_schema :somewhere, "postgres"
    @connection.create_proc('somewhere_else.afunc', []) { "BEGIN\nEND;" }

    @connection.create_schema :somewhere_else

    @connection.create_type(:qualitysmith_user, [:name, :varchar], {:address => "varchar(20)"}, [:zip, "varchar(5)"], [:phone, "numeric(10,0)"])
    @connection.create_proc("name-with-hyphen", [], :return => :trigger) { "  BEGIN\n--Something else goes here\nEND;\n" }
    @connection.create_proc(:update_trade_materials_statuses_logf, [], :return => :trigger) { "  BEGIN\n--Something else goes here\nEND;\n" }
    @connection.create_proc(:levenshtein, [], :return => :trigger, :resource => ['$libdir/fuzzystrmatch'], :lang => "c")
    @connection.add_trigger(:test_table, [:insert, :update], :row => true, :name => "update_trade-materials_statuses_logt", :function => :update_trade_materials_statuses_logf)
    @connection.add_trigger(:test_table, [:insert, :update], :function => :levenshtein)
    @connection.create_table("a_table_defined_after_the_stored_proc", :force => true) {|t|
      t.column :name, :varchar
    }
    @connection.drop_table "somewhere.table_in_a_specific_schema"
    @connection.create_table "somewhere.table_in_a_specific_schema" do |t|
      t.column :name, :text
    end
    @connection.create_proc(:sql_proc_with_table_reference, [:integer], :return => :integer, :lang => "SQL") { "SELECT id FROM a_table_defined_after_the_stored_proc WHERE id = $1;" }

    stream = StringIO.new
    ActiveRecord::SchemaDumper.ignore_tables = []
    ActiveRecord::SchemaDumper.dump(ActiveRecord::Base.connection, stream)
    received_sql = stream.string

    assert_match(%q|create_schema(:somewhere_else, "postgres")|.to_regex, received_sql)
    assert_match(%q|create_type(:qualitysmith_user, [:name, "character varying"], [:address, "character varying(20)"], [:zip, "character varying(5)"], [:phone, "numeric(10,0)"])|.to_regex, received_sql)
    assert_match(%q|create_proc('name-with-hyphen'|.to_regex, received_sql)
    assert_match(%q|create_proc(:update_trade_materials_statuses_logf|.to_regex, received_sql)
    assert_match(%q|create_proc("levenshtein", []|.to_regex, received_sql)
    assert_match(%q|add_trigger(:test_table, [:insert, :update], :function => :levenshtein)|.to_regex, received_sql)
    assert_match(%q|add_trigger(:test_table, [:insert, :update], :row => true, :name => 'update_trade-materials_statuses_logt', :function => :update_trade_materials_statuses_logf)|.to_regex, received_sql)
    assert_match(%q|create_table "a_table_defined_after_the_stored_proc"|.to_regex, received_sql)
    assert_match(%q|create_table("somewhere.schema_in_a_specific_table"|.to_regex, received_sql)
    assert_match(%q|create_proc(:sql_proc_with_table_reference, [:int4], :return => :int4, :lang => 'sql') {\n    <<-sql_proc_with_table_reference_sql\n\nSELECT id FROM a_table_defined_after_the_stored_proc WHERE id = $1;\n    sql_proc_with_table_reference_sql\n  }|.to_regex, received_sql.split("\n")[-9..-2].join("\n"))
    assert_match(%q|create_proc(:f_commacat, [:text, :text]|.to_regex, received_sql)
    assert_no_match(%q|lang => 'internal'|.to_regex, received_sql)
    assert_no_match(%q|create_proc(:comma|.to_regex, received_sql)
    assert_no_match(%q|create_proc(:afunc|.to_regex, received_sql)

    @connection.drop_proc(:sql_proc_with_table_reference, [:int4])
    @connection.drop_table("a_table_defined_after_the_stored_proc")
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
    @connection.drop_proc('somewhere_else.afunc', []) { "BEGIN\nEND;" }
    @connection.drop_schema "somewhere", :cascade => true
    @connection.drop_schema "somewhere_else", :cascade => true
    @connection.drop_proc(:f_commacat, [:text, :text])
  end

  def test_methods
    %w(procs types views schemas).each {|meth|
      %w(create drop).each {|action| 
        assert_respond_to "#{action}_#{meth.singularize}", @connection
      }
      assert_respond_to meth, @connection unless meth == 'procs'
    }
    %w(procedures triggers add_trigger remove_trigger).each {|meth|
      assert_respond_to meth, @connection
    }

    assert !@connection.procedures().nil?, "@connection#procedures returns nil"
    procedures_count = @connection.procedures.size
    trigger_count = @connection.triggers(:test_table).size
    with_proc(:insert_after_test_table_trigger, [], :return => :trigger) {
      assert_equal 0, trigger_count
      assert_equal procedures_count + 1, @connection.procedures.size
      with_trigger(:test_table, [:insert], :row => true) {
        assert !@connection.triggers(:test_table).nil?, "Triggers for table :test_table returns nil"
        received = @connection.triggers(:test_table)
        assert_equal trigger_count + 1, received.size
        assert_equal "insert_after_test_table_trigger", received.last.name
      }
    }
    assert_equal procedures_count, @connection.procedures.size
  end

  def test_more_complicated_schema_dumper
    with_proc(:levenshtein, [:text, :text], :return => nil, :resource => ['$libdir/fuzzystrmatch'], :strict => true, :behavior => 'immutable', :lang => "C") {
      assert_no_exception(NoMethodError) do 
        dumper = ActiveRecord::SchemaDumper.new(@connection)
        stream = StringIO.new
        dumper.send(:procedures, stream)
        stream.rewind
        received = stream.read
        assert_equal "  create_proc(\"levenshtein\", [:text, :text], :return => nil, :resource => ['$libdir/fuzzystrmatch', 'levenshtein'], :strict => true, :behavior => 'immutable', :lang => 'c')", received.split("\n")[-1]
      end
    }
  end

  def test_schema_definition_class
    schema = ActiveRecord::ConnectionAdapters::SchemaDefinition.new('rails', 'postgres')
    assert_match(/CREATE SCHEMA "rails" AUTHORIZATION "postgres"/, schema.to_sql)
    assert_equal('DROP SCHEMA "rails" RESTRICT', schema.to_sql(:drop))
    assert_match(/create_schema\ "rails", "postgres"$/, schema.to_rdl)

    count_query = "SELECT count(*) FROM pg_namespace WHERE nspname = 'rails'"
    assert_nil @connection.schemas.find {|schema| schema.name.to_s == 'rails' }
    assert_nothing_raised {
      @connection.create_schema "rails"
    }
#    assert_equal 'rails,"$user",public', @connection.schema_search_path
    assert_equal('"$user",public,rails', @connection.schema_search_path)
    assert_not_nil @connection.schemas.find {|schema| schema.name.to_s == 'rails' }
    assert_nothing_raised {
      @connection.drop_schema "rails"
    }
    assert_nil @connection.schemas.find {|schema| schema.name.to_s == 'rails' }
  end

  def test_schema_dumper_schema
    @connection.create_schema "rails"
    assert_no_exception(NoMethodError) do 
      dumper = ActiveRecord::SchemaDumper.new(@connection)
      stream = StringIO.new
      dumper.send(:schemas, stream)
      stream.rewind
      assert_match /create_schema\ "rails", "postgres"$/, stream.read.chomp 
    end
    @connection.drop_schema("rails", :cascade => true)
  end

  def test_schema_dumper_exceptions
    proc_name, columns = "test_sql_type_proc_with_table_reference", [:integer]
    assert_equal [], @connection.procedures
    assert_raise ActiveRecord::StatementInvalid do
      @connection.create_proc(proc_name, columns, :return => nil, :lang => :sql) { 
        <<-sql
          SELECT * FROM a_table_that_doesnt_yet_exist WHERE id = '$1';
        sql
      }
    end
    assert_equal [], @connection.procedures
    @connection.create_table(:a_table_that_doesnt_yet_exist, :force => true) { |t|
      t.column :name, :varchar
    }

    assert_equal [], @connection.procedures
    assert_nothing_raised do
      @connection.create_proc(proc_name, columns, :return => :integer, :lang => :sql, :force => true) { 
        <<-sql
          SELECT id FROM a_table_that_doesnt_yet_exist WHERE id = $1;
        sql
      }
    end
    @connection.drop_table(:a_table_that_doesnt_yet_exist)
    @connection.drop_proc(proc_name, columns)
    assert_equal [], @connection.procedures
  end

  def test_simple_schema_dumper
    with_proc(:insert_after_test_table_trigger, [], :return => :trigger) {
      with_trigger(:test_table, [:insert], :row => true) {
        assert_no_exception(NoMethodError) do 
          stream = StringIO.new
          dumper = ActiveRecord::SchemaDumper.new(@connection)
          dumper.send(:triggers, :test_table, stream)
          stream.rewind
          assert_match %q|add_trigger "test_table", [:insert], :row => true|.to_regex, stream.read

          stream = StringIO.new
          dumper.send(:procedures, stream)
          stream.rewind
          assert_match %Q|create_proc(\"insert_after_test_table_trigger\", [], :return => :trigger, :lang => 'plpgsql') {\n    <<-insert_after_test_table_trigger_sql\n\n#{@query_body}\n    insert_after_test_table_trigger_sql\n  }\n|.to_regex, stream.read
        end
      }
    }
  end

  def test_sym_to_str
    assert_equal '"abc"', "abc".to_sql_name
    assert_equal '"abc"', "abc".to_sym.to_sql_name
    assert_equal "'abc'", "abc".to_sym.to_sql_value
  end

  private
    def with_proc(name, columns=[], options={}, &block)
#        assert_equal [], @connection.procedures

        assert_nil @connection.procedures.find {|procedure| procedure[1] == name }
        if options[:resource]
            @connection.create_proc(name, columns, options)
        else
            @connection.create_proc(name, columns, options) { @query_body }
        end
        assert_equal name.to_s, @connection.procedures.last[1]
            yield
        @connection.drop_proc(name, columns)
#        assert_equal [], @connection.procedures
        assert_nil @connection.procedures.find {|procedure| procedure[1] == name }
    end

    def with_trigger(table, events=[], options={}, &block)
        @connection.add_trigger(table, events, options) 
            yield
        @connection.remove_trigger(table, options[:name] || ActiveSupport::Inflector.triggerize(table, events, options.has_key?(:before)))
    end
end
