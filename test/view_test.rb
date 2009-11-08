require 'test_helper'

class ViewTest < Test::Unit::TestCase
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
end
