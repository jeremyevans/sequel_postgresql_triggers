require 'sequel/extensions/pg_triggers'

module Sequel
  module Postgres
    module DatabaseMethods
      class_eval(&PGT_DEFINE)
    end
  end
end
