# The pg_triggers extension adds support to the Database instance for easily
# creating triggers and trigger returning functions for common needs.

#
module Sequel
  module Postgres
    PGT_DEFINE = proc do
      def pgt_counter_cache(main_table, main_table_id_column, counter_column, counted_table, counted_table_id_column, opts={})
        trigger_name = opts[:trigger_name] || "pgt_cc_#{pgt_mangled_table_name(main_table)}__#{main_table_id_column}__#{counter_column}__#{counted_table_id_column}"
        function_name = opts[:function_name] || "pgt_cc_#{pgt_mangled_table_name(main_table)}__#{main_table_id_column}__#{counter_column}__#{pgt_mangled_table_name(counted_table)}__#{counted_table_id_column}"

        table = quote_schema_table(main_table)
        id_column = quote_identifier(counted_table_id_column)
        main_column = quote_identifier(main_table_id_column)
        count_column = quote_identifier(counter_column)

        pgt_trigger(counted_table, trigger_name, function_name, [:insert, :update, :delete], <<-SQL, :after=>true)
        BEGIN
          #{pgt_pg_trigger_depth_guard_clause(opts)}
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
        function_name = opts[:function_name] || "pgt_ca_#{pgt_mangled_table_name(table)}__#{column}"
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

      def pgt_force_defaults(table, defaults, opts={})
        cols = defaults.keys.sort.join('_')
        trigger_name = opts[:trigger_name] || "pgt_fd_#{cols}"
        function_name = opts[:function_name] || "pgt_fd_#{pgt_mangled_table_name(table)}__#{cols}"
        lines = defaults.map do |column, v|
          "NEW.#{quote_identifier(column)} = #{literal(v)};"
        end
        pgt_trigger(table, trigger_name, function_name, [:insert], <<-SQL)
        BEGIN
          #{lines.join("\n")}
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

      def pgt_json_audit_log_setup(table, opts={})
        function_name = opts[:function_name] || "pgt_jal_#{pgt_mangled_table_name(table)}"
        create_table(table) do
          Bignum :txid, :null=>false, :index=>true
          DateTime :at, :default=>Sequel::CURRENT_TIMESTAMP, :null=>false
          String :user, :null=>false
          String :schema, :null=>false
          String :table, :null=>false
          String :action, :null=>false
          jsonb :prior, :null=>false
        end
        create_function(function_name, (<<-SQL), {:language=>:plpgsql, :returns=>:trigger, :replace=>true}.merge(opts[:function_opts]||{}))
        BEGIN
          #{pgt_pg_trigger_depth_guard_clause(opts)}
          INSERT INTO #{quote_schema_table(table)} (txid, at, "user", "schema", "table", action, prior) VALUES
          (txid_current(), CURRENT_TIMESTAMP, CURRENT_USER, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP, to_jsonb(OLD));
          IF (TG_OP = 'DELETE') THEN
            RETURN OLD;
          END IF;
          RETURN NEW;
        END;
        SQL
        function_name
      end

      def pgt_json_audit_log(table, function, opts={})
        create_trigger(table, (opts[:trigger_name] || "pgt_jal_#{pgt_mangled_table_name(table)}"), function, :events=>[:update, :delete], :each_row=>true, :after=>true)
      end

      def pgt_sum_cache(main_table, main_table_id_column, sum_column, summed_table, summed_table_id_column, summed_column, opts={})
        trigger_name = opts[:trigger_name] || "pgt_sc_#{pgt_mangled_table_name(main_table)}__#{main_table_id_column}__#{sum_column}__#{summed_table_id_column}"
        function_name = opts[:function_name] || "pgt_sc_#{pgt_mangled_table_name(main_table)}__#{main_table_id_column}__#{sum_column}__#{pgt_mangled_table_name(summed_table)}__#{summed_table_id_column}__#{summed_column}"

        table = quote_schema_table(main_table)
        id_column = quote_identifier(summed_table_id_column)

        new_table_summed_column = literal(Sequel.deep_qualify(Sequel.lit("NEW"), summed_column))
        old_table_summed_column = literal(Sequel.deep_qualify(Sequel.lit("OLD"), summed_column))
        main_column = quote_identifier(main_table_id_column)
        sum_column = quote_identifier(sum_column)

        pgt_trigger(summed_table, trigger_name, function_name, [:insert, :delete, :update], <<-SQL, :after=>true)
        BEGIN
          #{pgt_pg_trigger_depth_guard_clause(opts)}
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

        summed_column_slug = summed_column.is_a?(String) || summed_column.is_a?(Symbol) ? "__#{summed_column}" : ""
        trigger_name = opts[:trigger_name] || "pgt_stmc_#{pgt_mangled_table_name(main_table)}__#{main_table_id_column}__#{sum_column}__#{summed_table_id_column}#{summed_column_slug}__#{main_table_fk_column}__#{summed_table_fk_column}"
        function_name = opts[:function_name] || "pgt_stmc_#{pgt_mangled_table_name(main_table)}__#{main_table_id_column}__#{sum_column}__#{pgt_mangled_table_name(summed_table)}__#{summed_table_id_column}#{summed_column_slug}__#{pgt_mangled_table_name(join_table)}__#{main_table_fk_column}__#{summed_table_fk_column}"
        join_trigger_name = opts[:join_trigger_name] || "pgt_stmc_join_#{pgt_mangled_table_name(main_table)}__#{main_table_id_column}__#{sum_column}__#{summed_table_id_column}#{summed_column_slug}__#{main_table_fk_column}__#{summed_table_fk_column}"
        join_function_name = opts[:join_function_name] || "pgt_stmc_join_#{pgt_mangled_table_name(main_table)}__#{main_table_id_column}__#{sum_column}__#{pgt_mangled_table_name(summed_table)}__#{summed_table_id_column}#{summed_column_slug}__#{pgt_mangled_table_name(join_table)}__#{main_table_fk_column}__#{summed_table_fk_column}"

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

        pgt_trigger(orig_summed_table, trigger_name, function_name, [:insert, :delete, :update], <<-SQL, :after=>true)
        BEGIN
          #{pgt_pg_trigger_depth_guard_clause(opts)}
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

        pgt_trigger(orig_join_table, join_trigger_name, join_function_name, [:insert, :delete, :update], <<-SQL, :after=>true)
        BEGIN
          #{pgt_pg_trigger_depth_guard_clause(opts)}
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
        trigger_name = opts[:trigger_name] || "pgt_t_#{pgt_mangled_table_name(main_table)}__#{pgt_mangled_table_name(touch_table)}"
        function_name = opts[:function_name] || "pgt_t_#{pgt_mangled_table_name(main_table)}__#{pgt_mangled_table_name(touch_table)}"
        cond = lambda{|source| expr.map{|k,v| "#{quote_identifier(k)} = #{source}.#{quote_identifier(v)}"}.join(" AND ")}
        same_id = expr.map{|k,v| "NEW.#{quote_identifier(v)} = OLD.#{quote_identifier(v)}"}.join(" AND ")

        table = quote_schema_table(touch_table)
        col = quote_identifier(column)
        update = lambda{|source| " UPDATE #{table} SET #{col} = CURRENT_TIMESTAMP WHERE #{cond[source]} AND ((#{col} <> CURRENT_TIMESTAMP) OR (#{col} IS NULL));"}

        sql = <<-SQL
          BEGIN
            #{pgt_pg_trigger_depth_guard_clause(opts)}
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
        function_name = opts[:function_name] || "pgt_ua_#{pgt_mangled_table_name(table)}__#{column}"
        pgt_trigger(table, trigger_name, function_name, [:insert, :update], <<-SQL)
        BEGIN
          NEW.#{quote_identifier(column)} := CURRENT_TIMESTAMP;
          RETURN NEW;
        END;
        SQL
      end

      def pgt_foreign_key_array(opts={})
        table, column, rtable, rcolumn = opts.values_at(:table, :column, :referenced_table, :referenced_column)
        trigger_name = opts[:trigger_name] || "pgt_fka_#{column}"
        function_name = opts[:function_name] || "pgt_fka_#{pgt_mangled_table_name(table)}__#{column}"
        rtrigger_name = opts[:referenced_trigger_name] || "pgt_rfka_#{column}"
        rfunction_name = opts[:referenced_function_name] || "pgt_rfka_#{pgt_mangled_table_name(table)}__#{column}"
        col = quote_identifier(column)
        tab = quote_identifier(table)
        rcol = quote_identifier(rcolumn)
        rtab = quote_identifier(rtable)

        pgt_trigger(table, trigger_name, function_name, [:insert, :update], <<-SQL)
        DECLARE
          arr #{tab}.#{col}%TYPE;
          temp_count1 int;
          temp_count2 int;
        BEGIN
          arr := NEW.#{col};
          temp_count1 := array_ndims(arr);
          IF arr IS NULL OR temp_count1 IS NULL THEN
            RETURN NEW;
          END IF;

          IF temp_count1 IS DISTINCT FROM 1 THEN
              RAISE EXCEPTION 'Foreign key array #{tab}.#{col} has more than 1 dimension: %, dimensions: %', arr, temp_count1;
          END IF;

          SELECT count(*) INTO temp_count1 FROM unnest(arr);
          SELECT count(*) INTO temp_count2 FROM (SELECT DISTINCT * FROM unnest(arr)) AS t;
          IF temp_count1 IS DISTINCT FROM temp_count2 THEN
              RAISE EXCEPTION 'Duplicate entry in foreign key array #{tab}.#{col}: %', arr;
          END IF;

          SELECT COUNT(*) INTO temp_count1 FROM #{rtab} WHERE #{rcol} = ANY(arr);
          temp_count2 := array_length(arr, 1);
          IF temp_count1 IS DISTINCT FROM temp_count2 THEN
              RAISE EXCEPTION 'Entry in foreign key array #{tab}.#{col} not in referenced column #{rtab}.#{rcol}: %', arr;
          END IF;

          RETURN NEW;
        END;
        SQL

        pgt_trigger(rtable, rtrigger_name, rfunction_name, [:delete, :update], <<-SQL)
        DECLARE
          val #{rtab}.#{rcol}%TYPE;
          temp_count int;
        BEGIN
          val := OLD.#{rcol};
          IF (TG_OP = 'DELETE') OR val IS DISTINCT FROM NEW.#{rcol} THEN
            SELECT COUNT(*) INTO temp_count FROM #{tab} WHERE #{col} @> ARRAY[val];
            IF temp_count IS DISTINCT FROM 0 THEN
                RAISE EXCEPTION 'Entry in referenced column #{rtab}.#{rcol} still in foreign key array #{tab}.#{col}: %, count: %', val, temp_count;
            END IF;
          END IF;
          RETURN NEW;
        END;
        SQL
      end

      def pgt_outbox_setup(table, opts={})
        function_name = opts.fetch(:function_name, "pgt_outbox_#{pgt_mangled_table_name(table)}")
        outbox_table  = opts.fetch(:outbox_table, "#{table}_outbox")
        quoted_outbox = quote_schema_table(outbox_table)
        event_prefix  = opts.fetch(:event_prefix, table)
        created_column = opts.fetch(:created_column, :created)
        updated_column = opts.fetch(:updated_column, :updated)
        event_type_column = opts.fetch(:event_type_column, :event_type)
        data_after_column = opts.fetch(:data_after_column, :data_after)
        data_before_column = opts.fetch(:data_before_column, :data_before)
        boolean_completed_column = opts.fetch(:boolean_completed_column, false)
        uuid_primary_key = opts.fetch(:uuid_primary_key, false)
        run 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp"' if uuid_primary_key
        create_table(outbox_table) do
          if uuid_primary_key
            uuid_function = opts.fetch(:uuid_function, :uuid_generate_v4)
            uuid :id, default: Sequel.function(uuid_function), primary_key: true
          else
            primary_key :id
          end
          Integer opts.fetch(:attempts_column, :attempts), null: false, default: 0
          column  created_column, :timestamptz
          column  updated_column, :timestamptz
          column  opts.fetch(:attempted_column, :attempted), :timestamptz
          if boolean_completed_column
            FalseClass opts.fetch(:completed_column, :completed), null: false, default: false
          else
            column     opts.fetch(:completed_column, :completed), :timestamptz
          end
          String event_type_column, null: false
          String opts.fetch(:last_error_column, :last_error)
          jsonb  data_before_column
          jsonb  data_after_column
          jsonb  opts.fetch(:metadata_column, :metadata)
        end
        pgt_created_at outbox_table, created_column
        pgt_updated_at outbox_table, updated_column
        create_function(function_name, (<<-SQL), {:language=>:plpgsql, :returns=>:trigger, :replace=>true}.merge(opts[:function_opts]||{}))
        BEGIN
          #{pgt_pg_trigger_depth_guard_clause(opts)}
          IF (TG_OP = 'INSERT') THEN
              INSERT INTO #{quoted_outbox} ("#{event_type_column}", "#{data_after_column}") VALUES
              ('#{event_prefix}_created', to_jsonb(NEW));
              RETURN NEW;
          ELSIF (TG_OP = 'UPDATE') THEN
              INSERT INTO #{quoted_outbox} ("#{event_type_column}", "#{data_before_column}", "#{data_after_column}") VALUES
              ('#{event_prefix}_updated', to_jsonb(OLD), to_jsonb(NEW));
              RETURN NEW;
          ELSIF (TG_OP = 'DELETE') THEN
              INSERT INTO #{quoted_outbox} ("#{event_type_column}", "#{data_before_column}") VALUES
              ('#{event_prefix}_deleted', to_jsonb(OLD));
              RETURN OLD;
          END IF;
        END;
        SQL
        function_name
      end

      def pgt_outbox_events(table, function, opts={})
        events = opts.fetch(:events, [:insert, :update, :delete])
        trigger_name = opts.fetch(:trigger_name, "pgt_outbox_#{pgt_mangled_table_name(table)}")
        create_trigger(table, trigger_name, function, events: events, replace: true, each_row: true, after: true, when: opts[:when])
      end

      private

      # Add or replace a function that returns trigger to handle the action,
      # and add a trigger that calls the function.
      def pgt_trigger(table, trigger_name, function_name, events, definition, opts={})
        create_function(function_name, definition, :language=>:plpgsql, :returns=>:trigger, :replace=>true)
        create_trigger(table, trigger_name, function_name, :events=>events, :each_row=>true, :after=>opts[:after])
      end

      # Mangle the schema name so it can be used in an unquoted_identifier
      def pgt_mangled_table_name(table)
        quote_schema_table(table).gsub('"', '').gsub(/[^A-Za-z0-9]/, '_').gsub(/_+/, '_')
      end

      def pgt_pg_trigger_depth_guard_clause(opts)
        return unless depth_limit = opts[:trigger_depth_limit]
        depth_limit = 1 if true == depth_limit
        depth_limit = depth_limit.to_i
        raise ArgumentError, ":trigger_depth_limit option must be at least 1" unless depth_limit >= 1

        <<-SQL
        IF pg_trigger_depth() > #{depth_limit} THEN
            RETURN NEW;
          END IF;
        SQL
      end
    end

    module PGTMethods
      class_eval(&PGT_DEFINE)
    end
  end

  Database.register_extension(:pg_triggers, Postgres::PGTMethods)
end
