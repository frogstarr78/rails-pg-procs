print "Using native PostgreSQL\n"
require 'logger'

ActiveRecord::Base.logger = Logger.new("debug.log")

ActiveRecord::Base.configurations = {
  'rails_pg_procs' => {
    :adapter  => 'postgresql',
    :username => 'postgres',
    :database => 'activerecord_unittest',
    :min_messages => 'warning',
    :allow_concurrency => false
  }
}

ActiveRecord::Base.establish_connection('rails_pg_procs') unless ActiveRecord::Base.connected?
