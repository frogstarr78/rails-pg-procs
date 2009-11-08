require 'test_helper'

class TriggerTest < Test::Unit::TestCase
  def test_add_trigger
    trig = ActiveRecord::ConnectionAdapters::TriggerDefinition.new(0, "trade_materials", nil, [:insert, :update])
    assert_equal('CREATE TRIGGER "insert_or_update_after_trade_materials_trigger" AFTER INSERT OR UPDATE ON "trade_materials" FOR EACH STATEMENT EXECUTE PROCEDURE "insert_or_update_after_trade_materials_trigger"();', trig.to_sql_create)
    assert_equal('  add_trigger "trade_materials", [:insert, :update]', trig.to_rdl)

    trig = ActiveRecord::ConnectionAdapters::TriggerDefinition.new(0, "trade_materials", "update_trade_materials_statuses_logt", [:insert, :update])
    assert_equal('CREATE TRIGGER "update_trade_materials_statuses_logt" AFTER INSERT OR UPDATE ON "trade_materials" FOR EACH STATEMENT EXECUTE PROCEDURE "update_trade_materials_statuses_logt"();', trig.to_sql_create)
    assert_equal('  add_trigger "trade_materials", [:insert, :update], :name => :update_trade_materials_statuses_logt, :function => :update_trade_materials_statuses_logt', trig.to_rdl)

    trig = ActiveRecord::ConnectionAdapters::TriggerDefinition.new(0, "trade_materials", "update_trade_materials_statuses_logt", [:insert, :update, :before, :row], "update_trade_materials_statuses_logf")
    assert_equal('CREATE TRIGGER "update_trade_materials_statuses_logt" BEFORE INSERT OR UPDATE ON "trade_materials" FOR EACH ROW EXECUTE PROCEDURE "update_trade_materials_statuses_logf"();', trig.to_sql_create)
    assert_equal('  add_trigger "trade_materials", [:insert, :update], :before => true, :row => true, :name => :update_trade_materials_statuses_logt, :function => :update_trade_materials_statuses_logf', trig.to_rdl)

    trig = ActiveRecord::ConnectionAdapters::TriggerDefinition.new(0, "trade_materials", nil, [:insert, :update], "update_trade_materials_statuses_logf")
    assert_equal('CREATE TRIGGER "insert_or_update_after_trade_materials_trigger" AFTER INSERT OR UPDATE ON "trade_materials" FOR EACH STATEMENT EXECUTE PROCEDURE "update_trade_materials_statuses_logf"();', trig.to_sql_create)
    assert_equal('  add_trigger "trade_materials", [:insert, :update], :function => :update_trade_materials_statuses_logf', trig.to_rdl)

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

  def test_constants
    assert_equal 1<<0, ActiveRecord::ConnectionAdapters::TriggerDefinition::ROW
    assert_equal 1<<1, ActiveRecord::ConnectionAdapters::TriggerDefinition::BEFORE
    assert_equal 1<<2, ActiveRecord::ConnectionAdapters::TriggerDefinition::INSERT
    assert_equal 1<<3, ActiveRecord::ConnectionAdapters::TriggerDefinition::DELETE
    assert_equal 1<<4, ActiveRecord::ConnectionAdapters::TriggerDefinition::UPDATE
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

end
