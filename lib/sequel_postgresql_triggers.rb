module Sequel
  module Postgres
    # Add the pgt_* methods so that any Sequel database connecting to PostgreSQL
    # can use them.  All of these methods require the plpgsql procedural language
    # be added to the PostgreSQL database before they can be used. On PostgreSQL
    # 9.0 and later versions, it is installed by default.  For older versions,
    # you can install it with:
    #
    #   DB.create_language(:plpgsql)
    #
    # All of the public methods take the following options in their opts hash:
    #
    # * :function_name: The name of the function to use.  This is important
    #   to specify if you want an easy way to drop the function.
    # * :trigger_name: The name of the trigger to use.  This is important
    #   to specify if you want an easy way to drop the trigger.
    module DatabaseMethods
      # Turns a column in the main table into a counter cache.  A counter cache is a
      # column in the main table with the number of rows in the counted table
      # for the matching id. Arguments:
      # * main_table : name of table holding counter cache column
      # * main_table_id_column : column in main table matching counted_table_id_column in counted_table
      # * counter_column : column in main table containing the counter cache
      # * counted_table : name of table being counted
      # * counted_table_id_column : column in counted_table matching main_table_id_column in main_table
      # * opts : option hash, see module documentation
      def pgt_counter_cache(main_table, main_table_id_column, counter_column, counted_table, counted_table_id_column, opts={})
        trigger_name = opts[:trigger_name] || "pgt_cc_#{main_table}__#{main_table_id_column}__#{counter_column}__#{counted_table_id_column}"
        function_name = opts[:function_name] || "pgt_cc_#{main_table}__#{main_table_id_column}__#{counter_column}__#{counted_table}__#{counted_table_id_column}"

        table = quote_schema_table(main_table)
        id_column = quote_identifier(counted_table_id_column)
        main_column = quote_identifier(main_table_id_column)
        count_column = quote_identifier(counter_column)

        pgt_trigger(counted_table, trigger_name, function_name, [:insert, :update, :delete], <<-SQL)
        BEGIN
          IF (TG_OP = 'UPDATE' AND (NEW.#{id_column} = OLD.#{id_column} OR (OLD.#{id_column} IS NULL AND NEW.#{id_column} IS NULL))) THEN
            RETURN NEW;
          ELSE
            IF ((TG_OP = 'INSERT' OR TG_OP = 'UPDATE') AND NEW.#{id_column} IS NOT NULL) THEN
              UPDATE #{table} SET #{count_column} = #{count_column} + 1 WHERE #{main_column} = NEW.#{id_column};
            END IF;
            IF ((TG_OP = 'DELETE' OR TG_OP = 'UPDATE') AND OLD.#{id_column} IS NOT NULL) THEN
              UPDATE #{table} SET #{count_column} = #{count_column} - 1 WHERE #{main_column} = OLD.#{id_column};
            END IF;
          END IF;

          IF (TG_OP = 'DELETE') THEN
            RETURN OLD;
          END IF;
          RETURN NEW;
        END;
        SQL
      end

      # Turns a column in the table into a created at timestamp column, which
      # always contains the timestamp the record was inserted into the database.
      # Arguments:
      # * table : name of table
      # * column : column in table that should be a created at timestamp column
      # * opts : option hash, see module documentation
      def pgt_created_at(table, column, opts={})
        trigger_name = opts[:trigger_name] || "pgt_ca_#{column}"
        function_name = opts[:function_name] || "pgt_ca_#{table}__#{column}"
        col = quote_identifier(column)
        pgt_trigger(table, trigger_name, function_name, [:insert, :update], <<-SQL)
        BEGIN
          IF (TG_OP = 'UPDATE') THEN
            NEW.#{col} := OLD.#{col};
          ELSIF (TG_OP = 'INSERT') THEN
            NEW.#{col} := CURRENT_TIMESTAMP;
          END IF;
          RETURN NEW;
        END;
        SQL
      end

      # Makes all given columns in the given table immutable, so an exception
      # is raised if there is an attempt to modify the value when updating the
      # record. Arguments:
      # * table : name of table
      # * columns : All columns in the table that should be immutable.  Can end with a hash of options, see module documentation.
      def pgt_immutable(table, *columns)
        opts = columns.last.is_a?(Hash) ? columns.pop : {}
        trigger_name = opts[:trigger_name] || "pgt_im_#{columns.join('__')}"
        function_name = opts[:function_name] || "pgt_im_#{columns.join('__')}"
        ifs = columns.map do |c|
          old = "OLD.#{quote_identifier(c)}"
          new = "NEW.#{quote_identifier(c)}"
          <<-END
            IF #{new} IS DISTINCT FROM #{old} THEN
                RAISE EXCEPTION 'Attempted #{c} update: Old: %, New: %', #{old}, #{new};
            END IF;
          END
        end.join("\n")
        pgt_trigger(table, trigger_name, function_name, :update, "BEGIN #{ifs} RETURN NEW; END;")
      end

      # Turns a column in the main table into a sum cache.  A sum cache is a
      # column in the main table with the sum of a column in the summed table
      # for the matching id. Arguments:
      # * main_table : name of table holding counter cache column
      # * main_table_id_column : column in main table matching counted_table_id_column in counted_table
      # * sum_column : column in main table containing the sum cache
      # * summed_table : name of table being summed
      # * summed_table_id_column : column in summed_table matching main_table_id_column in main_table
      # * summed_column : column in summed_table being summed
      # * opts : option hash, see module documentation
      def pgt_sum_cache(main_table, main_table_id_column, sum_column, summed_table, summed_table_id_column, summed_column, opts={})
        trigger_name = opts[:trigger_name] || "pgt_sc_#{main_table}__#{main_table_id_column}__#{sum_column}__#{summed_table_id_column}"
        function_name = opts[:function_name] || "pgt_sc_#{main_table}__#{main_table_id_column}__#{sum_column}__#{summed_table}__#{summed_table_id_column}__#{summed_column}"

        table = quote_schema_table(main_table)
        id_column = quote_identifier(summed_table_id_column)
        summed_column = quote_identifier(summed_column)
        main_column = quote_identifier(main_table_id_column)
        sum_column = quote_identifier(sum_column)

        pgt_trigger(summed_table, trigger_name, function_name, [:insert, :delete, :update], <<-SQL)
        BEGIN
          IF (TG_OP = 'UPDATE' AND NEW.#{id_column} = OLD.#{id_column}) THEN
            UPDATE #{table} SET #{sum_column} = #{sum_column} + NEW.#{summed_column} - OLD.#{summed_column} WHERE #{main_column} = NEW.#{id_column};
          ELSE
            IF ((TG_OP = 'INSERT' OR TG_OP = 'UPDATE') AND NEW.#{id_column} IS NOT NULL) THEN
              UPDATE #{table} SET #{sum_column} = #{sum_column} + NEW.#{summed_column} WHERE #{main_column} = NEW.#{id_column};
            END IF;
            IF ((TG_OP = 'DELETE' OR TG_OP = 'UPDATE') AND OLD.#{id_column} IS NOT NULL) THEN
              UPDATE #{table} SET #{sum_column} = #{sum_column} - OLD.#{summed_column} WHERE #{main_column} = OLD.#{id_column};
            END IF;
          END IF;
          IF (TG_OP = 'DELETE') THEN
            RETURN OLD;
          END IF;
          RETURN NEW;
        END;
        SQL
      end

      # Turns a column in the main table into a sum cache through a join table.
      # A sum cache is a column in the main table with the sum of a column in the
      # summed table for the matching id. Arguments:
      # * main_table : name of table holding counter cache column
      # * main_table_id_column : column in main table matching main_table_fk_column in join_table
      # * sum_column : column in main table containing the sum cache
      # * summed_table : name of table being summed
      # * summed_table_id_column : column in summed_table matching summed_table_fk_column in join_table
      # * summed_column : column in summed_table being summed
      # * join_table : name of table which joins main_table with summed_table
      # * main_table_fk_column : column in join_table matching main_table_id_column in main_table
      # * summed_table_fk_column : column in join_table matching summed_table_id_column in summed_table
      # * opts : option hash, see module documentation
      def pgt_sum_through_many_cache(main_table, main_table_id_column, sum_column, summed_table, summed_table_id_column, summed_column, join_table, main_table_fk_column, summed_table_fk_column, opts={})
        trigger_name = opts[:trigger_name] || "pgt_stmc_#{main_table}__#{main_table_id_column}__#{sum_column}__#{summed_table_id_column}__#{summed_column}__#{main_table_fk_column}__#{summed_table_fk_column}"
        function_name = opts[:function_name] || "pgt_stmc_#{main_table}__#{main_table_id_column}__#{sum_column}__#{summed_table}__#{summed_table_id_column}__#{summed_column}__#{join_table}__#{main_table_fk_column}__#{summed_table_fk_column}"
        join_trigger_name = opts[:join_trigger_name] || "pgt_stmc_join_#{main_table}__#{main_table_id_column}__#{sum_column}__#{summed_table_id_column}__#{summed_column}__#{main_table_fk_column}__#{summed_table_fk_column}"
        join_function_name = opts[:join_function_name] || "pgt_stmc_join_#{main_table}__#{main_table_id_column}__#{sum_column}__#{summed_table}__#{summed_table_id_column}__#{summed_column}__#{join_table}__#{main_table_fk_column}__#{summed_table_fk_column}"

        orig_summed_table = summed_table
        orig_join_table = join_table

        main_table = quote_schema_table(main_table)
        main_table_id_column = quote_schema_table(main_table_id_column)
        sum_column = quote_schema_table(sum_column)
        summed_table = quote_schema_table(summed_table)
        summed_table_id_column = quote_schema_table(summed_table_id_column)
        summed_column = quote_schema_table(summed_column)
        join_table = quote_schema_table(join_table)
        main_table_fk_column = quote_schema_table(main_table_fk_column)
        summed_table_fk_column = quote_schema_table(summed_table_fk_column)

        pgt_trigger(orig_summed_table, trigger_name, function_name, [:insert, :delete, :update], <<-SQL)
        BEGIN
          IF (TG_OP = 'UPDATE' AND NEW.#{summed_table_id_column} = OLD.#{summed_table_id_column}) THEN
            UPDATE #{main_table} SET #{sum_column} = #{sum_column} + NEW.#{summed_column} - OLD.#{summed_column} WHERE #{main_table_id_column} IN (SELECT #{main_table_fk_column} FROM #{join_table} WHERE #{summed_table_fk_column} = NEW.#{summed_table_id_column});
          ELSE
            IF ((TG_OP = 'INSERT' OR TG_OP = 'UPDATE') AND NEW.#{summed_table_id_column} IS NOT NULL) THEN
              UPDATE #{main_table} SET #{sum_column} = #{sum_column} + NEW.#{summed_column} WHERE #{main_table_id_column} IN (SELECT #{main_table_fk_column} FROM #{join_table} WHERE #{summed_table_fk_column} = NEW.#{summed_table_id_column});
            END IF;
            IF ((TG_OP = 'DELETE' OR TG_OP = 'UPDATE') AND OLD.#{summed_table_id_column} IS NOT NULL) THEN
              UPDATE #{main_table} SET #{sum_column} = #{sum_column} - OLD.#{summed_column} WHERE #{main_table_id_column} IN (SELECT #{main_table_fk_column} FROM #{join_table} WHERE #{summed_table_fk_column} = OLD.#{summed_table_id_column});
            END IF;
          END IF;
          IF (TG_OP = 'DELETE') THEN
            RETURN OLD;
          END IF;
          RETURN NEW;
        END;
        SQL

        pgt_trigger(orig_join_table, join_trigger_name, join_function_name, [:insert, :delete, :update], <<-SQL)
        BEGIN
          IF (TG_OP = 'UPDATE' AND NEW.#{main_table_fk_column} = OLD.#{main_table_fk_column} AND NEW.#{summed_table_fk_column} = OLD.#{summed_table_fk_column}) THEN
          ELSE
            IF (TG_OP = 'INSERT' OR TG_OP = 'UPDATE') THEN
              IF (NEW.#{main_table_fk_column} IS NOT NULL AND NEW.#{summed_table_fk_column} IS NOT NULL) THEN
                UPDATE #{main_table} SET #{sum_column} = #{sum_column} + (SELECT SUM(#{summed_column}) AS #{summed_column} FROM #{summed_table} WHERE #{summed_table_id_column} = NEW.#{summed_table_fk_column}) WHERE #{main_table_id_column} = NEW.#{main_table_fk_column};
              END IF;
            END IF;
            IF (TG_OP = 'DELETE' OR TG_OP = 'UPDATE') THEN
              IF (OLD.#{main_table_fk_column} IS NOT NULL AND OLD.#{summed_table_fk_column} IS NOT NULL) THEN
                UPDATE #{main_table} SET #{sum_column} = #{sum_column} - (SELECT SUM(#{summed_column}) AS #{summed_column} FROM #{summed_table} WHERE #{summed_table_id_column} = OLD.#{summed_table_fk_column}) WHERE #{main_table_id_column} = OLD.#{main_table_fk_column};
              END IF;
            END IF;
          END IF;
          IF (TG_OP = 'DELETE') THEN
            RETURN OLD;
          END IF;
          RETURN NEW;
        END;
        SQL
      end

      # When rows in a table are updated, touches a timestamp of related rows
      # in another table.
      # Arguments:
      # * main_table : name of table that is being watched for changes
      # * touch_table : name of table that needs to be touched
      # * column : name of timestamp column to be touched
      # * expr : hash or array that represents the columns that define the relationship
      # * opts : option hash, see module documentation
      def pgt_touch(main_table, touch_table, column, expr, opts={})
        trigger_name = opts[:trigger_name] || "pgt_t_#{main_table}__#{touch_table}"
        function_name = opts[:function_name] || "pgt_t_#{main_table}__#{touch_table}"
        cond = lambda{|source| expr.map{|k,v| "#{quote_identifier(k)} = #{source}.#{quote_identifier(v)}"}.join(" AND ")}
        same_id = expr.map{|k,v| "NEW.#{quote_identifier(v)} = OLD.#{quote_identifier(v)}"}.join(" AND ")

        table = quote_schema_table(touch_table)
        col = quote_identifier(column)
        update = lambda{|source| " UPDATE #{table} SET #{col} = CURRENT_TIMESTAMP WHERE #{cond[source]} AND ((#{col} <> CURRENT_TIMESTAMP) OR (#{col} IS NULL));"}

        sql = <<-SQL
          BEGIN
            IF (TG_OP = 'UPDATE' AND (#{same_id})) THEN
              #{update['NEW']}
            ELSE
              IF (TG_OP = 'INSERT' OR TG_OP = 'UPDATE') THEN
                #{update['NEW']}
              END IF;
              IF (TG_OP = 'DELETE' OR TG_OP = 'UPDATE') THEN
                #{update['OLD']}
              END IF;
            END IF;

            IF (TG_OP = 'DELETE') THEN
              RETURN OLD;
            END IF;
            RETURN NEW;
          END;
        SQL
        pgt_trigger(main_table, trigger_name, function_name, [:insert, :delete, :update], sql, :after=>true)
      end

      # Turns a column in the table into a updated at timestamp column, which
      # always contains the timestamp the record was inserted or last updated.
      # Arguments:
      # * table : name of table
      # * column : column in table that should be a updated at timestamp column
      # * opts : option hash, see module documentation
      def pgt_updated_at(table, column, opts={})
        trigger_name = opts[:trigger_name] || "pgt_ua_#{column}"
        function_name = opts[:function_name] || "pgt_ua_#{table}__#{column}"
        pgt_trigger(table, trigger_name, function_name, [:insert, :update], <<-SQL)
        BEGIN
          NEW.#{quote_identifier(column)} := CURRENT_TIMESTAMP;
          RETURN NEW;
        END;
        SQL
      end

      private

      # Add or replace a function that returns trigger to handle the action,
      # and add a trigger that calls the function.
      def pgt_trigger(table, trigger_name, function_name, events, definition, opts={})
        create_function(function_name, definition, :language=>:plpgsql, :returns=>:trigger, :replace=>true)
        create_trigger(table, trigger_name, function_name, :events=>events, :each_row=>true, :after=>opts[:after])
      end
    end
  end
end
