# The pg_triggers extension adds support to the Database instance for easily
# creating triggers and trigger returning functions for common needs.

#
module Sequel
  module Postgres
    PGT_DEFINE = proc do
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

      def pgt_sum_cache(main_table, main_table_id_column, sum_column, summed_table, summed_table_id_column, summed_column, opts={})
        trigger_name = opts[:trigger_name] || "pgt_sc_#{main_table}__#{main_table_id_column}__#{sum_column}__#{summed_table_id_column}"
        function_name = opts[:function_name] || "pgt_sc_#{main_table}__#{main_table_id_column}__#{sum_column}__#{summed_table}__#{summed_table_id_column}__#{summed_column}"

        table = quote_schema_table(main_table)
        id_column = quote_identifier(summed_table_id_column)

        new_table_summed_column = literal(Sequel.deep_qualify(Sequel.lit("NEW"), summed_column))
        old_table_summed_column = literal(Sequel.deep_qualify(Sequel.lit("OLD"), summed_column))
        main_column = quote_identifier(main_table_id_column)
        sum_column = quote_identifier(sum_column)

        pgt_trigger(summed_table, trigger_name, function_name, [:insert, :delete, :update], <<-SQL)
        BEGIN
          IF (TG_OP = 'UPDATE' AND NEW.#{id_column} = OLD.#{id_column}) THEN
            UPDATE #{table} SET #{sum_column} = #{sum_column} + #{new_table_summed_column} - #{old_table_summed_column} WHERE #{main_column} = NEW.#{id_column};
          ELSE
            IF ((TG_OP = 'INSERT' OR TG_OP = 'UPDATE') AND NEW.#{id_column} IS NOT NULL) THEN
              UPDATE #{table} SET #{sum_column} = #{sum_column} + #{new_table_summed_column} WHERE #{main_column} = NEW.#{id_column};
            END IF;
            IF ((TG_OP = 'DELETE' OR TG_OP = 'UPDATE') AND OLD.#{id_column} IS NOT NULL) THEN
              UPDATE #{table} SET #{sum_column} = #{sum_column} - #{old_table_summed_column} WHERE #{main_column} = OLD.#{id_column};
            END IF;
          END IF;
          IF (TG_OP = 'DELETE') THEN
            RETURN OLD;
          END IF;
          RETURN NEW;
        END;
        SQL
      end

      def pgt_sum_through_many_cache(opts={})
        main_table = opts.fetch(:main_table)
        main_table_id_column = opts.fetch(:main_table_id_column, :id)
        sum_column = opts.fetch(:sum_column)
        summed_table = opts.fetch(:summed_table)
        summed_table_id_column = opts.fetch(:summed_table_id_column, :id)
        summed_column = opts.fetch(:summed_column)
        join_table = opts.fetch(:join_table)
        main_table_fk_column = opts.fetch(:main_table_fk_column)
        summed_table_fk_column = opts.fetch(:summed_table_fk_column)

        trigger_name = opts[:trigger_name] || "pgt_stmc_#{main_table}__#{main_table_id_column}__#{sum_column}__#{summed_table_id_column}__#{main_table_fk_column}__#{summed_table_fk_column}"
        function_name = opts[:function_name] || "pgt_stmc_#{main_table}__#{main_table_id_column}__#{sum_column}__#{summed_table}__#{summed_table_id_column}__#{summed_column}__#{join_table}__#{main_table_fk_column}__#{summed_table_fk_column}"
        join_trigger_name = opts[:join_trigger_name] || "pgt_stmc_join_#{main_table}__#{main_table_id_column}__#{sum_column}__#{summed_table_id_column}__#{main_table_fk_column}__#{summed_table_fk_column}"
        join_function_name = opts[:join_function_name] || "pgt_stmc_join_#{main_table}__#{main_table_id_column}__#{sum_column}__#{summed_table}__#{summed_table_id_column}__#{summed_column}__#{join_table}__#{main_table_fk_column}__#{summed_table_fk_column}"

        orig_summed_table = summed_table
        orig_join_table = join_table

        main_table = quote_schema_table(main_table)
        main_table_id_column = quote_schema_table(main_table_id_column)
        sum_column = quote_schema_table(sum_column)

        general_summed_column = literal(Sequel.deep_qualify(summed_table, summed_column))
        new_table_summed_column = literal(Sequel.deep_qualify(Sequel.lit("NEW"), summed_column))
        old_table_summed_column = literal(Sequel.deep_qualify(Sequel.lit("OLD"), summed_column))

        summed_table = quote_schema_table(summed_table)
        summed_table_id_column = quote_schema_table(summed_table_id_column)
        join_table = quote_schema_table(join_table)
        main_table_fk_column = quote_schema_table(main_table_fk_column)
        summed_table_fk_column = quote_schema_table(summed_table_fk_column)

        pgt_trigger(orig_summed_table, trigger_name, function_name, [:insert, :delete, :update], <<-SQL)
        BEGIN
          IF (TG_OP = 'UPDATE' AND NEW.#{summed_table_id_column} = OLD.#{summed_table_id_column}) THEN
            UPDATE #{main_table} SET #{sum_column} = #{sum_column} + #{new_table_summed_column} - #{old_table_summed_column} WHERE #{main_table_id_column} IN (SELECT #{main_table_fk_column} FROM #{join_table} WHERE #{summed_table_fk_column} = NEW.#{summed_table_id_column});
          ELSE
            IF (TG_OP = 'INSERT' OR TG_OP = 'UPDATE') THEN
              UPDATE #{main_table} SET #{sum_column} = #{sum_column} + #{new_table_summed_column} WHERE #{main_table_id_column} IN (SELECT #{main_table_fk_column} FROM #{join_table} WHERE #{summed_table_fk_column} = NEW.#{summed_table_id_column});
            END IF;
            IF (TG_OP = 'DELETE' OR TG_OP = 'UPDATE') THEN
              UPDATE #{main_table} SET #{sum_column} = #{sum_column} - #{old_table_summed_column} WHERE #{main_table_id_column} IN (SELECT #{main_table_fk_column} FROM #{join_table} WHERE #{summed_table_fk_column} = OLD.#{summed_table_id_column});
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
          IF (NOT (TG_OP = 'UPDATE' AND NEW.#{main_table_fk_column} = OLD.#{main_table_fk_column} AND NEW.#{summed_table_fk_column} = OLD.#{summed_table_fk_column})) THEN
            IF (TG_OP = 'INSERT' OR TG_OP = 'UPDATE') THEN
              UPDATE #{main_table} SET #{sum_column} = #{sum_column} + (SELECT #{general_summed_column} FROM #{summed_table} WHERE #{summed_table_id_column} = NEW.#{summed_table_fk_column}) WHERE #{main_table_id_column} = NEW.#{main_table_fk_column};
            END IF;
            IF (TG_OP = 'DELETE' OR TG_OP = 'UPDATE') THEN
              UPDATE #{main_table} SET #{sum_column} = #{sum_column} - (SELECT #{general_summed_column} FROM #{summed_table} WHERE #{summed_table_id_column} = OLD.#{summed_table_fk_column}) WHERE #{main_table_id_column} = OLD.#{main_table_fk_column};
            END IF;
          END IF;
          IF (TG_OP = 'DELETE') THEN
            RETURN OLD;
          END IF;
          RETURN NEW;
        END;
        SQL
      end

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

    module PGTMethods
      class_eval(&PGT_DEFINE)
    end
  end

  Database.register_extension(:pg_triggers, Postgres::PGTMethods)
end
