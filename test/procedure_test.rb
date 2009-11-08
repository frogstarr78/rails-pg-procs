require 'test_helper'

class ProcedureTest < Test::Unit::TestCase
  def test_create_proc
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
      Regexp.new(@query_body),
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
      Regexp.new(@query_body),
      /LANGUAGE SQL/,
    ].each {|re| 
      assert_match(re, @connection.send("get_proc_query", "update_trade_materials_statuses_logf", [], :return => nil, :lang => :SQL) { @query_body })
    }

    [
      /update_trade_materials_statuses_logf()/,
      /RETURNS trigger/,
      /\$update_trade_materials_statuses_logf_body\$/,
      Regexp.new(@query_body),
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
end
