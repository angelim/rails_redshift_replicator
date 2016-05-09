module RailsRedshiftReplicator
  module Tools
    class Vacuum
      # Executes Redshift's VACUUM command to reclaim space
      # @param table [String, nil] table to perform the command or :all
      # @param options [String, nil] command options: FULL, SORT ONLY, DELETE ONLY, REINDEX, auto_tune
      # @note auto_tune chooses the best VACUUM strategy based on each table sort key style.
      # see [http://docs.aws.amazon.com/redshift/latest/dg/r_VACUUM_command.html]
      def initialize(table = nil, options = nil)
        @table = table
        @options = options
      end

      def perform
        if @options == "auto_tune"
          auto_tuned_vacuum(@table)
        else
          @table = (@table.blank? || @table.to_s == "all") ? nil : @table
          command = "VACUUM #{@options} #{@table};".squish
          RailsRedshiftReplicator.logger.debug(command)
          RailsRedshiftReplicator.connection.exec command
        end
      end

      
      # (see .exec_vacuum_command)
      def auto_tuned_vacuum(table)
        if table.to_s == "all"
          sort_types.each do |line|
            exec_vacuum_command line['tablename'], line['sort_type']
          end
        else
          exec_vacuum_command table, sort_type_for_table(table)
        end
      end

      # @see .vacuum
      def exec_vacuum_command(table, sort_type)
        command = if sort_type.in? ["compound", nil]
          "VACUUM FULL #{table};"
                  else
          "VACUUM REINDEX #{table};"
        end.squish
        RailsRedshiftReplicator.logger.debug(command)
        RailsRedshiftReplicator.connection.exec command
      end

      # Finds which sort keys are present on a set of tables
      def sort_types
        @sort_types ||= begin
          sql = <<-SQLT
            SELECT
            CASE
              WHEN min(sortkey) < 0 THEN 'interleaved'
              ELSE 'compound'
            END AS sort_type, tablename
            FROM pg_table_def
            WHERE tablename in (#{tables_for_sql})
            GROUP BY tablename
          SQLT
          RailsRedshiftReplicator.connection.exec(sql.squish).entries
        end
      end

      # @param table_name [String] table name
      # @return ["compound", "interleaved"] sort key style
      def sort_type_for_table(table_name)
        sort_types.select{ |el| el["tablename"] == table_name }.first.try(:fetch, "sort_type") || "compound"
      end
      # Lists tables to replicate
      # @return [String> table names
      def tables_for_sql
        @tables_for_sql ||= RailsRedshiftReplicator.replicable_target_tables.join(",")
      end
    end
  end
end