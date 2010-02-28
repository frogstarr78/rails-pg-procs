require 'test_helper'

class TypeTest < Test::Unit::TestCase
  def test_create_type
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

  def test_schema_dumper_type
    # a type test
    @connection.create_type("qualitysmith_user", [:name, "varchar(10)"], {:zip => "numeric(5,0)"}, [:is_customer => :boolean])
    assert_no_exception(NoMethodError) do 
      dumper = ActiveRecord::SchemaDumper.new(@connection)
      stream = StringIO.new
      dumper.send(:types, stream)
      stream.rewind
      received = stream.read
      ['[:name, "character varying(10)"]', '[:zip, "numeric(5,0)"]', '[:is_customer, :boolean]'].each do |fragment|
        assert received.chomp[%r|create_type "qualitysmith_user",(.*)|, 1].include?(fragment)
      end
       
    end
    @connection.drop_type(:qualitysmith_user)
  end
end
