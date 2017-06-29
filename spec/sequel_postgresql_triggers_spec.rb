#!/usr/bin/env ruby
require 'rubygems'
require 'sequel'
require 'minitest/spec'
require 'minitest/autorun'

DB = Sequel.connect(ENV['PGT_SPEC_DB']||'postgres:///spgt_test?user=postgres')

$:.unshift(File.join(File.dirname(File.dirname(File.expand_path(__FILE__))), 'lib'))
if ENV['PGT_GLOBAL'] == '1'
  puts "Running specs with global modification"
  require 'sequel_postgresql_triggers'
else
  puts "Running specs with extension"
  DB.extension :pg_triggers 
end

describe "PostgreSQL Triggers" do
  before do
    DB.create_language(:plpgsql) if DB.server_version < 90000
  end
  after do
    DB.drop_language(:plpgsql, :cascade=>true) if DB.server_version < 90000
  end

  describe "PostgreSQL Counter Cache Trigger" do
    before do
      DB.create_table(:accounts){integer :id; integer :num_entries, :default=>0}
      DB.create_table(:entries){integer :id; integer :account_id}
      DB.pgt_counter_cache(:accounts, :id, :num_entries, :entries, :account_id, :function_name=>:spgt_counter_cache)
      DB[:accounts].insert(:id=>1)
      DB[:accounts].insert(:id=>2)
    end

    after do
      DB.drop_table(:entries, :accounts)
      DB.drop_function(:spgt_counter_cache)
    end

    it "Should modify counter cache when adding or removing records" do
      DB[:accounts].order(:id).select_map(:num_entries).must_equal [0, 0]

      DB[:entries].insert(:id=>1, :account_id=>1)
      DB[:accounts].order(:id).select_map(:num_entries).must_equal [1, 0]

      DB[:entries].insert(:id=>2, :account_id=>1)
      DB[:accounts].order(:id).select_map(:num_entries).must_equal [2, 0]
      
      DB[:entries].insert(:id=>3, :account_id=>nil)
      DB[:accounts].order(:id).select_map(:num_entries).must_equal [2, 0]
      
      DB[:entries].where(:id=>3).update(:account_id=>2)
      DB[:accounts].order(:id).select_map(:num_entries).must_equal [2, 1]
      
      DB[:entries].where(:id=>2).update(:account_id=>2)
      DB[:accounts].order(:id).select_map(:num_entries).must_equal [1, 2]
      
      DB[:entries].where(:id=>2).update(:account_id=>nil)
      DB[:accounts].order(:id).select_map(:num_entries).must_equal [1, 1]
      
      DB[:entries].where(:id=>2).update(:id=>4)
      DB[:accounts].order(:id).select_map(:num_entries).must_equal [1, 1]
      
      DB[:entries].where(:id=>4).update(:account_id=>2)
      DB[:accounts].order(:id).select_map(:num_entries).must_equal [1, 2]
      
      DB[:entries].where(:id=>4).update(:account_id=>nil)
      DB[:accounts].order(:id).select_map(:num_entries).must_equal [1, 1]
      
      DB[:entries].filter(:id=>4).delete
      DB[:accounts].order(:id).select_map(:num_entries).must_equal [1, 1]
      
      DB[:entries].delete
      DB[:accounts].order(:id).select_map(:num_entries).must_equal [0, 0]
    end
  end

  describe "PostgreSQL Created At Trigger" do
    before do
      DB.create_table(:accounts){integer :id; timestamp :added_on}
      DB.pgt_created_at(:accounts, :added_on, :function_name=>:spgt_created_at)
    end

    after do
      DB.drop_table(:accounts)
      DB.drop_function(:spgt_created_at)
    end

    it "Should set the column upon insertion and ignore modifications afterward" do
      DB[:accounts].insert(:id=>1)
      t = DB[:accounts].get(:added_on)
      t.strftime('%F').must_equal Date.today.strftime('%F')
      DB[:accounts].update(:added_on=>Date.today - 60)
      DB[:accounts].get(:added_on).must_equal t
      DB[:accounts].insert(:id=>2)
      ds = DB[:accounts].select(:added_on)
      DB[:accounts].select((Sequel::SQL::NumericExpression.new(:NOOP, ds.filter(:id=>2)) > ds.filter(:id=>1)).as(:x)).first[:x].must_equal true
      DB[:accounts].filter(:id=>1).update(:id=>3)
      DB[:accounts].select((Sequel::SQL::NumericExpression.new(:NOOP, ds.filter(:id=>2)) > ds.filter(:id=>3)).as(:x)).first[:x].must_equal true
    end
  end

  describe "PostgreSQL Immutable Trigger" do
    before do
      DB.create_table(:accounts){integer :id; integer :balance, :default=>0}
      DB.pgt_immutable(:accounts, :balance, :function_name=>:spgt_immutable)
      DB[:accounts].insert(:id=>1)
    end

    after do
      DB.drop_table(:accounts)
      DB.drop_function(:spgt_immutable)
    end

    it "Should allow modifying columns not marked as immutable" do
      DB[:accounts].update(:id=>2)
    end

    it "Should allow updating a column to its existing value" do
      DB[:accounts].update(:balance=>0)
      DB[:accounts].update(:balance=>Sequel.*(:balance, :balance))
    end

    it "Should not allow modifying a column's value" do
      proc{DB[:accounts].update(:balance=>1)}.must_raise(Sequel::DatabaseError)
    end

    it "Should handle NULL values correctly" do
      proc{DB[:accounts].update(:balance=>nil)}.must_raise(Sequel::DatabaseError)
      DB[:accounts].delete
      DB[:accounts].insert(:id=>1, :balance=>nil)
      DB[:accounts].update(:balance=>nil)
      proc{DB[:accounts].update(:balance=>0)}.must_raise(Sequel::DatabaseError)
    end
  end

  describe "PostgreSQL Sum Cache Trigger" do
    before do
      DB.create_table(:accounts){integer :id; integer :balance, :default=>0}
      DB.create_table(:entries){integer :id; integer :account_id; integer :amount}
      DB.pgt_sum_cache(:accounts, :id, :balance, :entries, :account_id, :amount, :function_name=>:spgt_sum_cache)
      DB[:accounts].insert(:id=>1)
      DB[:accounts].insert(:id=>2)
    end

    after do
      DB.drop_table(:entries, :accounts)
      DB.drop_function(:spgt_sum_cache)
    end

    it "Should modify sum cache when adding, updating, or removing records" do
      DB[:accounts].order(:id).select_map(:balance).must_equal [0, 0]

      DB[:entries].insert(:id=>1, :account_id=>1, :amount=>100)
      DB[:accounts].order(:id).select_map(:balance).must_equal [100, 0]

      DB[:entries].insert(:id=>2, :account_id=>1, :amount=>200)
      DB[:accounts].order(:id).select_map(:balance).must_equal [300, 0]
      
      DB[:entries].insert(:id=>3, :account_id=>nil, :amount=>500)
      DB[:accounts].order(:id).select_map(:balance).must_equal [300, 0]
      
      DB[:entries].where(:id=>3).update(:account_id=>2)
      DB[:accounts].order(:id).select_map(:balance).must_equal [300, 500]
      
      DB[:entries].exclude(:id=>2).update(:amount=>Sequel.*(:amount, 2))
      DB[:accounts].order(:id).select_map(:balance).must_equal [400, 1000]
      
      DB[:entries].where(:id=>2).update(:account_id=>2)
      DB[:accounts].order(:id).select_map(:balance).must_equal [200, 1200]
      
      DB[:entries].where(:id=>2).update(:account_id=>nil)
      DB[:accounts].order(:id).select_map(:balance).must_equal [200, 1000]
      
      DB[:entries].where(:id=>2).update(:id=>4)
      DB[:accounts].order(:id).select_map(:balance).must_equal [200, 1000]
      
      DB[:entries].where(:id=>4).update(:account_id=>2)
      DB[:accounts].order(:id).select_map(:balance).must_equal [200, 1200]
      
      DB[:entries].where(:id=>4).update(:account_id=>nil)
      DB[:accounts].order(:id).select_map(:balance).must_equal [200, 1000]
      
      DB[:entries].filter(:id=>4).delete
      DB[:accounts].order(:id).select_map(:balance).must_equal [200, 1000]
      
      DB[:entries].delete
      DB[:accounts].order(:id).select_map(:balance).must_equal [0, 0]
    end
  end

  describe "PostgreSQL Sum Cache Trigger with arbitrary expression" do
    before do
      DB.create_table(:accounts){integer :id; integer :nonzero_entries_count, :default=>0}
      DB.create_table(:entries){integer :id; integer :account_id; integer :amount}
      DB.pgt_sum_cache(:accounts, :id, :nonzero_entries_count, :entries, :account_id, Sequel.case({0=>0}, 1, :amount), :function_name=>:spgt_sum_cache)
      DB[:accounts].insert(:id=>1)
      DB[:accounts].insert(:id=>2)
    end

    after do
      DB.drop_table(:entries, :accounts)
      DB.drop_function(:spgt_sum_cache)
    end

    it "Should modify sum cache when adding, updating, or removing records" do
      DB[:accounts].order(:id).select_map(:nonzero_entries_count).must_equal [0, 0]

      DB[:entries].insert(:id=>1, :account_id=>1, :amount=>100)
      DB[:accounts].order(:id).select_map(:nonzero_entries_count).must_equal [1, 0]

      DB[:entries].insert(:id=>2, :account_id=>1, :amount=>200)
      DB[:accounts].order(:id).select_map(:nonzero_entries_count).must_equal [2, 0]

      DB[:entries].insert(:id=>3, :account_id=>nil, :amount=>500)
      DB[:accounts].order(:id).select_map(:nonzero_entries_count).must_equal [2, 0]

      DB[:entries].where(:id=>3).update(:account_id=>2)
      DB[:accounts].order(:id).select_map(:nonzero_entries_count).must_equal [2, 1]

      DB[:entries].exclude(:id=>2).update(:amount=>Sequel.*(:amount, 2))
      DB[:accounts].order(:id).select_map(:nonzero_entries_count).must_equal [2, 1]

      DB[:entries].where(:id=>2).update(:account_id=>2)
      DB[:accounts].order(:id).select_map(:nonzero_entries_count).must_equal [1, 2]

      DB[:entries].where(:id=>2).update(:account_id=>nil)
      DB[:accounts].order(:id).select_map(:nonzero_entries_count).must_equal [1, 1]

      DB[:entries].where(:id=>2).update(:id=>4)
      DB[:accounts].order(:id).select_map(:nonzero_entries_count).must_equal [1, 1]

      DB[:entries].where(:id=>4).update(:account_id=>2)
      DB[:accounts].order(:id).select_map(:nonzero_entries_count).must_equal [1, 2]

      DB[:entries].where(:id=>4).update(:account_id=>nil)
      DB[:accounts].order(:id).select_map(:nonzero_entries_count).must_equal [1, 1]

      DB[:entries].filter(:id=>4).delete
      DB[:accounts].order(:id).select_map(:nonzero_entries_count).must_equal [1, 1]

      DB[:entries].delete
      DB[:accounts].order(:id).select_map(:nonzero_entries_count).must_equal [0, 0]
    end
  end


  describe "PostgreSQL Sum Through Many Cache Trigger" do
    before do
      DB.create_table(:parents){primary_key :id; integer :balance, :default=>0, :null=>false}
      DB.create_table(:children){primary_key :id; integer :amount, :null=>false}
      DB.create_table(:links){integer :parent_id, :null=>false; integer :child_id, :null=>false; unique [:parent_id, :child_id]}
      DB.pgt_sum_through_many_cache(
        :main_table=>:parents,
        :sum_column=>:balance,
        :summed_table=>:children,
        :summed_column=>:amount,
        :join_table=>:links,
        :main_table_fk_column=>:parent_id,
        :summed_table_fk_column=>:child_id,
        :function_name=>:spgt_stm_cache,
        :join_function_name=>:spgt_stm_cache_join
      )
      DB[:parents].insert(:id=>1)
      DB[:parents].insert(:id=>2)
    end

    after do
      DB.drop_table(:links, :parents, :children)
      DB.drop_function(:spgt_stm_cache)
      DB.drop_function(:spgt_stm_cache_join)
    end

    it "Should modify sum cache when adding, updating, or removing records" do
      DB[:parents].order(:id).select_map(:balance).must_equal [0, 0]

      DB[:children].insert(:id=>1, :amount=>100)
      DB[:links].insert(:parent_id=>1, :child_id=>1)
      DB[:parents].order(:id).select_map(:balance).must_equal [100, 0]

      DB[:children].insert(:id=>2, :amount=>200)
      DB[:links].insert(:parent_id=>1, :child_id=>2)
      DB[:parents].order(:id).select_map(:balance).must_equal [300, 0]

      DB[:children].insert(:id=>3, :amount=>500)
      DB[:parents].order(:id).select_map(:balance).must_equal [300, 0]
      DB[:links].insert(:parent_id=>2, :child_id=>3)
      DB[:parents].order(:id).select_map(:balance).must_equal [300, 500]

      DB[:links].where(:parent_id=>2, :child_id=>3).update(:parent_id=>1)
      DB[:parents].order(:id).select_map(:balance).must_equal [800, 0]

      DB[:children].insert(:id=>4, :amount=>400)
      DB[:links].where(:parent_id=>1, :child_id=>3).update(:child_id=>4)
      DB[:parents].order(:id).select_map(:balance).must_equal [700, 0]

      DB[:links].where(:parent_id=>1, :child_id=>4).update(:parent_id=>2, :child_id=>3)
      DB[:parents].order(:id).select_map(:balance).must_equal [300, 500]

      DB[:children].exclude(:id=>2).update(:amount=>Sequel.*(:amount, 2))
      DB[:parents].order(:id).select_map(:balance).must_equal [400, 1000]

      DB[:links].where(:parent_id=>1, :child_id=>2).update(:parent_id=>2)
      DB[:parents].order(:id).select_map(:balance).must_equal [200, 1200]

      DB[:links].where(:parent_id=>2, :child_id=>2).update(:parent_id=>1)
      DB[:parents].order(:id).select_map(:balance).must_equal [400, 1000]

      DB[:links].where(:parent_id=>1, :child_id=>2).update(:child_id=>3)
      DB[:parents].order(:id).select_map(:balance).must_equal [1200, 1000]

      DB[:links].insert(:parent_id=>2, :child_id=>4)
      DB[:parents].order(:id).select_map(:balance).must_equal [1200, 1800]

      DB[:children].filter(:id=>4).delete
      DB[:parents].order(:id).select_map(:balance).must_equal [1200, 1000]

      DB[:links].filter(:parent_id=>1, :child_id=>1).delete
      DB[:parents].order(:id).select_map(:balance).must_equal [1000, 1000]

      DB[:children].insert(:id=>4, :amount=>400)
      DB[:parents].order(:id).select_map(:balance).must_equal [1000, 1400]

      DB[:children].delete
      DB[:parents].order(:id).select_map(:balance).must_equal [0, 0]

      DB[:children].multi_insert([{:id=>2, :amount=>200}, {:id=>1, :amount=>200}, {:id=>3, :amount=>1000}, {:id=>4, :amount=>400}])
      DB[:parents].order(:id).select_map(:balance).must_equal [1000, 1400]

      DB[:links].where(:child_id=>3).update(:child_id=>2)
      DB[:parents].order(:id).select_map(:balance).must_equal [200, 600]

      DB[:children].update(:amount=>10)
      DB[:parents].order(:id).select_map(:balance).must_equal [10, 20]

      DB[:links].delete
      DB[:parents].order(:id).select_map(:balance).must_equal [0, 0]
    end
  end

  describe "PostgreSQL Sum Through Many Cache Trigger with arbitrary expression" do
    before do
      DB.create_table(:parents){primary_key :id; integer :nonzero_entries_count, :default=>0, :null=>false}
      DB.create_table(:children){primary_key :id; integer :amount, :null=>false}
      DB.create_table(:links){integer :parent_id, :null=>false; integer :child_id, :null=>false; unique [:parent_id, :child_id]}
      DB.pgt_sum_through_many_cache(
        :main_table=>:parents,
        :sum_column=>:nonzero_entries_count,
        :summed_table=>:children,
        :summed_column=>Sequel.case({0=>0}, 1, :amount),
        :join_table=>:links,
        :main_table_fk_column=>:parent_id,
        :summed_table_fk_column=>:child_id,
        :function_name=>:spgt_stm_cache,
        :join_function_name=>:spgt_stm_cache_join
      )
      DB[:parents].insert(:id=>1)
      DB[:parents].insert(:id=>2)
    end

    after do
      DB.drop_table(:links, :parents, :children)
      DB.drop_function(:spgt_stm_cache)
      DB.drop_function(:spgt_stm_cache_join)
    end

    it "Should modify sum cache when adding, updating, or removing records" do
      DB[:parents].order(:id).select_map(:nonzero_entries_count).must_equal [0, 0]

      DB[:children].insert(:id=>1, :amount=>100)
      DB[:links].insert(:parent_id=>1, :child_id=>1)
      DB[:parents].order(:id).select_map(:nonzero_entries_count).must_equal [1, 0]

      DB[:children].insert(:id=>2, :amount=>200)
      DB[:links].insert(:parent_id=>1, :child_id=>2)
      DB[:parents].order(:id).select_map(:nonzero_entries_count).must_equal [2, 0]

      DB[:children].insert(:id=>3, :amount=>500)
      DB[:parents].order(:id).select_map(:nonzero_entries_count).must_equal [2, 0]
      DB[:links].insert(:parent_id=>2, :child_id=>3)
      DB[:parents].order(:id).select_map(:nonzero_entries_count).must_equal [2, 1]

      DB[:links].where(:parent_id=>2, :child_id=>3).update(:parent_id=>1)
      DB[:parents].order(:id).select_map(:nonzero_entries_count).must_equal [3, 0]

      DB[:children].insert(:id=>4, :amount=>400)
      DB[:links].where(:parent_id=>1, :child_id=>3).update(:child_id=>4)
      DB[:parents].order(:id).select_map(:nonzero_entries_count).must_equal [3, 0]

      DB[:links].where(:parent_id=>1, :child_id=>4).update(:parent_id=>2, :child_id=>3)
      DB[:parents].order(:id).select_map(:nonzero_entries_count).must_equal [2, 1]

      DB[:children].exclude(:id=>2).update(:amount=>Sequel.*(:amount, 2))
      DB[:parents].order(:id).select_map(:nonzero_entries_count).must_equal [2, 1]

      DB[:links].where(:parent_id=>1, :child_id=>2).update(:parent_id=>2)
      DB[:parents].order(:id).select_map(:nonzero_entries_count).must_equal [1, 2]

      DB[:links].where(:parent_id=>2, :child_id=>2).update(:parent_id=>1)
      DB[:parents].order(:id).select_map(:nonzero_entries_count).must_equal [2, 1]

      DB[:links].where(:parent_id=>1, :child_id=>2).update(:child_id=>3)
      DB[:parents].order(:id).select_map(:nonzero_entries_count).must_equal [2, 1]

      DB[:links].insert(:parent_id=>2, :child_id=>4)
      DB[:parents].order(:id).select_map(:nonzero_entries_count).must_equal [2, 2]

      DB[:children].filter(:id=>4).delete
      DB[:parents].order(:id).select_map(:nonzero_entries_count).must_equal [2, 1]

      DB[:links].filter(:parent_id=>1, :child_id=>1).delete
      DB[:parents].order(:id).select_map(:nonzero_entries_count).must_equal [1, 1]

      DB[:children].insert(:id=>4, :amount=>400)
      DB[:parents].order(:id).select_map(:nonzero_entries_count).must_equal [1, 2]

      DB[:children].delete
      DB[:parents].order(:id).select_map(:nonzero_entries_count).must_equal [0, 0]

      DB[:children].multi_insert([{:id=>2, :amount=>200}, {:id=>1, :amount=>200}, {:id=>3, :amount=>1000}, {:id=>4, :amount=>400}])
      DB[:parents].order(:id).select_map(:nonzero_entries_count).must_equal [1, 2]

      DB[:links].where(:child_id=>3).update(:child_id=>2)
      DB[:parents].order(:id).select_map(:nonzero_entries_count).must_equal [1, 2]

      DB[:children].update(:amount=>10)
      DB[:parents].order(:id).select_map(:nonzero_entries_count).must_equal [1, 2]

      DB[:links].delete
      DB[:parents].order(:id).select_map(:nonzero_entries_count).must_equal [0, 0]
    end
  end

  describe "PostgreSQL Updated At Trigger" do
    before do
      DB.create_table(:accounts){integer :id; timestamp :changed_on}
      DB.pgt_updated_at(:accounts, :changed_on, :function_name=>:spgt_updated_at)
    end

    after do
      DB.drop_table(:accounts)
      DB.drop_function(:spgt_updated_at)
    end

    it "Should set the column always to the current timestamp" do
      DB[:accounts].insert(:id=>1)
      t = DB[:accounts].get(:changed_on)
      t.strftime('%F').must_equal Date.today.strftime('%F')
      DB[:accounts].insert(:id=>2)
      ds = DB[:accounts].select(:changed_on)
      DB[:accounts].select((Sequel::SQL::NumericExpression.new(:NOOP, ds.filter(:id=>2)) > ds.filter(:id=>1)).as(:x)).first[:x].must_equal true
      DB[:accounts].filter(:id=>1).update(:id=>3)
      DB[:accounts].select((Sequel::SQL::NumericExpression.new(:NOOP, ds.filter(:id=>3)) > ds.filter(:id=>2)).as(:x)).first[:x].must_equal true
    end
  end

  describe "PostgreSQL Touch Trigger" do
    before do
      DB.create_table(:parents){integer :id1; integer :id2; integer :child_id; timestamp :changed_on}
      DB.create_table(:children){integer :id; integer :parent_id1; integer :parent_id2; timestamp :changed_on}
    end

    after do
      DB.drop_table(:children, :parents)
      DB.drop_function(:spgt_touch)
      DB.drop_function(:spgt_touch2) if @spgt_touch2
    end

    it "Should update the timestamp column of the related table when adding, updating or removing records" do
      DB.pgt_touch(:children, :parents, :changed_on, {:id1=>:parent_id1}, :function_name=>:spgt_touch)
      d = Date.today
      d30 = Date.today - 30
      DB[:parents].insert(:id1=>1, :changed_on=>d30)
      DB[:parents].insert(:id1=>2, :changed_on=>d30)
      DB[:children].insert(:id=>1, :parent_id1=>1)
      DB[:parents].order(:id1).select_map(:changed_on).map{|t| t.strftime('%F')}.must_equal [d.strftime('%F'), d30.strftime('%F')]

      DB[:parents].update(:changed_on=>d30)
      DB[:children].update(:id=>2)
      DB[:parents].order(:id1).select_map(:changed_on).map{|t| t.strftime('%F')}.must_equal [d.strftime('%F'), d30.strftime('%F')]

      DB[:parents].update(:changed_on=>d30)
      DB[:children].update(:parent_id1=>2)
      DB[:parents].order(:id1).select_map(:changed_on).map{|t| t.strftime('%F')}.must_equal [d.strftime('%F'), d.strftime('%F')]

      DB[:parents].update(:changed_on=>d30)
      DB[:children].update(:parent_id1=>nil)
      DB[:parents].order(:id1).select_map(:changed_on).map{|t| t.strftime('%F')}.must_equal [d30.strftime('%F'), d.strftime('%F')]

      DB[:parents].update(:changed_on=>d30)
      DB[:children].update(:parent_id2=>1)
      DB[:parents].order(:id1).select_map(:changed_on).map{|t| t.strftime('%F')}.must_equal [d30.strftime('%F'), d30.strftime('%F')]

      DB[:parents].update(:changed_on=>d30)
      DB[:children].update(:parent_id1=>2)
      DB[:parents].order(:id1).select_map(:changed_on).map{|t| t.strftime('%F')}.must_equal [d30.strftime('%F'), d.strftime('%F')]

      DB[:parents].update(:changed_on=>d30)
      DB[:children].delete
      DB[:parents].order(:id1).select_map(:changed_on).map{|t| t.strftime('%F')}.must_equal [d30.strftime('%F'), d.strftime('%F')]

      DB[:parents].update(:changed_on=>d30)
      DB[:children].insert(:id=>2, :parent_id1=>nil)
      DB[:parents].order(:id1).select_map(:changed_on).map{|t| t.strftime('%F')}.must_equal [d30.strftime('%F'), d30.strftime('%F')]
      DB[:children].where(:id=>2).delete
      DB[:parents].order(:id1).select_map(:changed_on).map{|t| t.strftime('%F')}.must_equal [d30.strftime('%F'), d30.strftime('%F')]
    end

    it "Should update the timestamp column of the related table when there is a composite foreign key" do
      DB.pgt_touch(:children, :parents, :changed_on, {:id1=>:parent_id1, :id2=>:parent_id2}, :function_name=>:spgt_touch)
      DB[:parents].insert(:id1=>1, :id2=>2, :changed_on=>Date.today - 30)
      DB[:children].insert(:id=>1, :parent_id1=>1, :parent_id2=>2)
      DB[:parents].get(:changed_on).strftime('%F').must_equal Date.today.strftime('%F')
      DB[:parents].update(:changed_on=>Date.today - 30)
      DB[:children].update(:id=>2)
      DB[:parents].get(:changed_on).strftime('%F').must_equal Date.today.strftime('%F')
      DB[:parents].update(:changed_on=>Date.today - 30)
      DB[:children].delete
      DB[:parents].get(:changed_on).strftime('%F').must_equal Date.today.strftime('%F')
    end

    it "Should update timestamps correctly when two tables touch each other" do
      DB.pgt_touch(:children, :parents, :changed_on, {:id1=>:parent_id1}, :function_name=>:spgt_touch)
      @spgt_touch2 = true
      DB.pgt_touch(:parents, :children, :changed_on, {:id=>:child_id}, :function_name=>:spgt_touch2)
      DB[:parents].insert(:id1=>1, :child_id=>1, :changed_on=>Date.today - 30)
      DB[:children].insert(:id=>1, :parent_id1=>1, :changed_on=>Date.today - 30)
      DB[:parents].get(:changed_on).strftime('%F').must_equal Date.today.strftime('%F')
      DB[:children].get(:changed_on).strftime('%F').must_equal Date.today.strftime('%F')
      time = DB[:parents].get(:changed_on)
      DB[:parents].update(:id2=>4)
      DB[:parents].get(:changed_on).must_be :>,  time
      DB[:children].get(:changed_on).must_be :>,  time
      time = DB[:parents].get(:changed_on)
      DB[:children].update(:id=>1)
      DB[:parents].get(:changed_on).must_be :>,  time
      DB[:children].get(:changed_on).must_be :>,  time
      time = DB[:parents].get(:changed_on)
      DB[:children].delete
      DB[:parents].get(:changed_on).must_be :>,  time
    end

    it "Should update the timestamp on the related table if that timestamp is initially NULL" do
      DB.pgt_touch(:children, :parents, :changed_on, {:id1=>:parent_id1}, :function_name=>:spgt_touch)
      DB[:parents].insert(:id1=>1, :changed_on=>nil)
      DB[:children].insert(:id=>1, :parent_id1=>1)
      changed_on = DB[:parents].get(:changed_on)
      changed_on.wont_equal nil
      changed_on.strftime('%F').must_equal Date.today.strftime('%F')
    end
  end
end
