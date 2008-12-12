module Sequel
  module Postgres
    # Add the pgt_* methods so that any Sequel database connecting to PostgreSQL
    # can use them.  All of these methods require the plpgsql procedural language
    # added to the PostgreSQL database before they can be used.  You can do so
    # with:
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
        pgt_trigger(counted_table, trigger_name, function_name, [:insert, :delete], <<-SQL)
        BEGIN
          IF (TG_OP = 'DELETE') THEN
            UPDATE #{quote_schema_table(main_table)} SET #{quote_identifier(counter_column)} = #{quote_identifier(counter_column)} - 1 WHERE #{quote_identifier(main_table_id_column)} = OLD.#{counted_table_id_column};
            RETURN OLD;
          ELSIF (TG_OP = 'INSERT') THEN
            UPDATE #{quote_schema_table(main_table)} SET #{quote_identifier(counter_column)} = #{quote_identifier(counter_column)} + 1 WHERE #{quote_identifier(main_table_id_column)} = NEW.#{quote_identifier(counted_table_id_column)};
            RETURN NEW;
          END IF;
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
        pgt_trigger(table, trigger_name, function_name, [:insert, :update], <<-SQL)
        BEGIN
          IF (TG_OP = 'UPDATE') THEN
            NEW.#{quote_identifier(column)} := OLD.#{quote_identifier(column)};
          ELSIF (TG_OP = 'INSERT') THEN
            NEW.#{quote_identifier(column)} := CURRENT_TIMESTAMP;
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
        opts = columns.extract_options!
        trigger_name = opts[:trigger_name] || "pgt_im_#{columns.join('__')}"
        function_name = opts[:function_name] || "pgt_im_#{columns.join('__')}"
        ifs = columns.map do |c|
          old = "OLD.#{quote_identifier(c)}"
          new = "NEW.#{quote_identifier(c)}"
          <<-END
            IF #{new} != #{old} THEN
                RAISE EXCEPTION 'Attempted event_id update: Old: %, New: %', #{old}, #{new};
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
        pgt_trigger(summed_table, trigger_name, function_name, [:insert, :delete, :update], <<-SQL)
        BEGIN
          IF (TG_OP = 'DELETE') THEN
            UPDATE #{quote_schema_table(main_table)} SET #{quote_identifier(sum_column)} = #{quote_identifier(sum_column)} - OLD.#{quote_identifier(summed_column)} WHERE #{quote_identifier(main_table_id_column)} = OLD.#{summed_table_id_column};
            RETURN OLD;
          ELSIF (TG_OP = 'UPDATE') THEN
            UPDATE #{quote_schema_table(main_table)} SET #{quote_identifier(sum_column)} = #{quote_identifier(sum_column)} + NEW.#{quote_identifier(summed_column)} - OLD.#{quote_identifier(summed_column)} WHERE #{quote_identifier(main_table_id_column)} = NEW.#{quote_identifier(summed_table_id_column)};
            RETURN NEW;
          ELSIF (TG_OP = 'INSERT') THEN
            UPDATE #{quote_schema_table(main_table)} SET #{quote_identifier(sum_column)} = #{quote_identifier(sum_column)} + NEW.#{quote_identifier(summed_column)} WHERE #{quote_identifier(main_table_id_column)} = NEW.#{quote_identifier(summed_table_id_column)};
            RETURN NEW;
          END IF;
        END;
        SQL
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
      def pgt_trigger(table, trigger_name, function_name, events, definition)
        create_function(function_name, definition, :language=>:plpgsql, :returns=>:trigger, :replace=>true)
        create_trigger(table, trigger_name, function_name, :events=>events, :each_row=>true)
      end
    end
  end
end