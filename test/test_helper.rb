require 'rubygems'
require 'stringio'
require 'runit/assert'
require 'test/unit'

$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require 'rails_pg_procs'
require 'connection'

class ActiveRecord::SchemaDumper; public_class_method :new; end
# we want new public for the tests so we can test each method independantly from the rest

class Test::Unit::TestCase
  @_use_transactional_fixtures = false

  include RUNIT::Assert
  def setup
    @connection = ActiveRecord::Base.connection
    @connection.create_table(:test_table, :force => true) {|t|
      t.text      :name
      t.timestamp :when
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
end

class String
  def to_regex
    Regexp.new(self.gsub(/([\s\n\t]+)/, '\\1+').gsub(/([\(\)\[\]\{\}\.\\\$])/) {|s| '\\' + s })
  end
end
