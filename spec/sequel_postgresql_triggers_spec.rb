#!/usr/bin/env spec
require 'rubygems'
require 'sequel'

DB = Sequel.connect(ENV['PGT_SPEC_DB']||'postgres:///spgt_test?user=postgres')

$:.unshift(File.join(File.dirname(File.dirname(File.expand_path(__FILE__))), 'lib'))
require 'sequel_postgresql_triggers'

context "PostgreSQL Triggers" do
before do
  DB.create_language(:plpgsql) if DB.server_version < 90000
end
after do
  DB.drop_language(:plpgsql, :cascade=>true) if DB.server_version < 90000
end

context "PostgreSQL Counter Cache Trigger" do
  before do
    DB.create_table(:accounts){integer :id; integer :num_entries, :default=>0}
    DB.create_table(:entries){integer :id; integer :account_id}
    DB.pgt_counter_cache(:accounts, :id, :num_entries, :entries, :account_id)
    DB[:accounts] << {:id=>1}
    DB[:accounts] << {:id=>2}
  end

  after do
    DB.drop_table(:entries, :accounts)
  end

  specify "Should modify counter cache when adding or removing records" do
    DB[:accounts].filter(:id=>1).get(:num_entries).should == 0
    DB[:accounts].filter(:id=>2).get(:num_entries).should == 0
    DB[:entries] << {:id=>1, :account_id=>1}
    DB[:accounts].filter(:id=>1).get(:num_entries).should == 1
    DB[:accounts].filter(:id=>2).get(:num_entries).should == 0
    DB[:entries] << {:id=>2, :account_id=>1}
    DB[:accounts].filter(:id=>1).get(:num_entries).should == 2
    DB[:accounts].filter(:id=>2).get(:num_entries).should == 0
    DB[:entries] << {:id=>3, :account_id=>2}
    DB[:accounts].filter(:id=>1).get(:num_entries).should == 2
    DB[:accounts].filter(:id=>2).get(:num_entries).should == 1
    DB[:entries].filter(:id=>2).delete
    DB[:accounts].filter(:id=>1).get(:num_entries).should == 1
    DB[:accounts].filter(:id=>2).get(:num_entries).should == 1
    DB[:entries].delete
    DB[:accounts].filter(:id=>1).get(:num_entries).should == 0
    DB[:accounts].filter(:id=>2).get(:num_entries).should == 0
  end
end

context "PostgreSQL Created At Trigger" do
  before do
    DB.create_table(:accounts){integer :id; timestamp :added_on}
    DB.pgt_created_at(:accounts, :added_on)
  end

  after do
    DB.drop_table(:accounts)
  end

  specify "Should set the column upon insertion and ignore modifications afterward" do
    DB[:accounts] << {:id=>1}
    t = DB[:accounts].get(:added_on)
    t.strftime('%F').should == Date.today.strftime('%F')
    DB[:accounts].update(:added_on=>Date.today - 60)
    DB[:accounts].get(:added_on).should == t
    DB[:accounts] << {:id=>2}
    ds = DB[:accounts].select(:added_on)
    DB[:accounts].select((Sequel::SQL::NumericExpression.new(:NOOP, ds.filter(:id=>2)) > ds.filter(:id=>1)).as(:x)).first[:x].should == true
    DB[:accounts].filter(:id=>1).update(:id=>3)
    DB[:accounts].select((Sequel::SQL::NumericExpression.new(:NOOP, ds.filter(:id=>2)) > ds.filter(:id=>3)).as(:x)).first[:x].should == true
  end
end

context "PostgreSQL Immutable Trigger" do
  before do
    DB.create_table(:accounts){integer :id; integer :balance, :default=>0}
    DB.pgt_immutable(:accounts, :balance)
    DB[:accounts] << {:id=>1}
  end

  after do
    DB.drop_table(:accounts)
  end

  specify "Should allow modifying columns not marked as immutable" do
    proc{DB[:accounts].update(:id=>2)}.should_not raise_error
  end

  specify "Should allow updating a column to its existing value" do
    proc{DB[:accounts].update(:balance=>0)}.should_not raise_error
    proc{DB[:accounts].update(:balance=>:balance * :balance)}.should_not raise_error
  end

  specify "Should not allow modifying a column's value" do
    proc{DB[:accounts].update(:balance=>1)}.should raise_error(Sequel::DatabaseError)
  end

  specify "Should handle NULL values correctly" do
    proc{DB[:accounts].update(:balance=>nil)}.should raise_error(Sequel::DatabaseError)
    DB[:accounts].delete
    DB[:accounts] << {:id=>1, :balance=>nil}
    proc{DB[:accounts].update(:balance=>nil)}.should_not raise_error
    proc{DB[:accounts].update(:balance=>0)}.should raise_error(Sequel::DatabaseError)
  end
end

context "PostgreSQL Sum Cache Trigger" do
  before do
    DB.create_table(:accounts){integer :id; integer :balance, :default=>0}
    DB.create_table(:entries){integer :id; integer :account_id; integer :amount}
    DB.pgt_sum_cache(:accounts, :id, :balance, :entries, :account_id, :amount)
    DB[:accounts] << {:id=>1}
    DB[:accounts] << {:id=>2}
  end

  after do
    DB.drop_table(:entries, :accounts)
  end

  specify "Should modify sum cache when adding, updating, or removing records" do
    DB[:accounts].filter(:id=>1).get(:balance).should == 0
    DB[:accounts].filter(:id=>2).get(:balance).should == 0
    DB[:entries] << {:id=>1, :account_id=>1, :amount=>100}
    DB[:accounts].filter(:id=>1).get(:balance).should == 100
    DB[:accounts].filter(:id=>2).get(:balance).should == 0
    DB[:entries] << {:id=>2, :account_id=>1, :amount=>200}
    DB[:accounts].filter(:id=>1).get(:balance).should == 300
    DB[:accounts].filter(:id=>2).get(:balance).should == 0
    DB[:entries] << {:id=>3, :account_id=>2, :amount=>500}
    DB[:accounts].filter(:id=>1).get(:balance).should == 300
    DB[:accounts].filter(:id=>2).get(:balance).should == 500
    DB[:entries].exclude(:id=>2).update(:amount=>:amount * 2)
    DB[:accounts].filter(:id=>1).get(:balance).should == 400
    DB[:accounts].filter(:id=>2).get(:balance).should == 1000
    DB[:entries].filter(:id=>2).delete
    DB[:accounts].filter(:id=>1).get(:balance).should == 200
    DB[:accounts].filter(:id=>2).get(:balance).should == 1000
    DB[:entries].delete
    DB[:accounts].filter(:id=>1).get(:balance).should == 0
    DB[:accounts].filter(:id=>2).get(:balance).should == 0
  end
end

context "PostgreSQL Updated At Trigger" do
  before do
    DB.create_table(:accounts){integer :id; timestamp :changed_on}
    DB.pgt_updated_at(:accounts, :changed_on)
  end

  after do
    DB.drop_table(:accounts)
  end

  specify "Should set the column always to the current timestamp" do
    DB[:accounts] << {:id=>1}
    t = DB[:accounts].get(:changed_on)
    t.strftime('%F').should == Date.today.strftime('%F')
    DB[:accounts] << {:id=>2}
    ds = DB[:accounts].select(:changed_on)
    DB[:accounts].select((Sequel::SQL::NumericExpression.new(:NOOP, ds.filter(:id=>2)) > ds.filter(:id=>1)).as(:x)).first[:x].should == true
    DB[:accounts].filter(:id=>1).update(:id=>3)
    DB[:accounts].select((Sequel::SQL::NumericExpression.new(:NOOP, ds.filter(:id=>3)) > ds.filter(:id=>2)).as(:x)).first[:x].should == true
  end
end
end
