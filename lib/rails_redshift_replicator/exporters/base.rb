require 'csv'
require 'rails_redshift_replicator/adapters/generic'
require 'rails_redshift_replicator/adapters/mysql2'
require 'rails_redshift_replicator/adapters/postgresql'
require 'rails_redshift_replicator/adapters/sqlite'

module RailsRedshiftReplicator
  module Exporters
    class Base
      extend Forwardable
      def_delegators :replicable, :replication_type, :source_table, :target_table, :replication_field, :exporter_class
      attr_reader :replicable
      attr_accessor :replication, :file_names, :errors

      def initialize(replicable, current_replication = nil)
        @replicable = replicable
        @replication = current_replication
        @file_names = nil
        @errors = nil
        check_target_table
        check_index
      end

      # Exports and uploads selected records from the source_table
      def export_and_upload(options = {})
        RailsRedshiftReplicator::Deleter.new(replicable).handle_delete_propagation
        files = export options
        upload files
        replication
      end

      # Lists indexes from source table
      # @return [Array<String>] indexes from source table
      def table_indexes
        ActiveRecord::Base.connection.indexes(source_table).map{ |table| table.columns}.flatten | ["id"]
      end

      # Verifies if the table has the recommended indexes to increase export performance.
      # @return [true, false] if table has recommended indexes
      def has_index?
        replication_field.in? table_indexes
      end

      # Reports missing indexes
      # @see #has_index? 
      def check_index
        if !has_index? && replication_field.present?
          RailsRedshiftReplicator.logger.warn I18n.t(:missing_indexes, replication_field: replication_field, table_name: source_table, scope: :rails_redshift_replicator)
        end
      end

      # Checks if target table exists on Redshift
      # @return [true, false] if table exists
      def check_target_table
        unless fields_to_sync
          message = I18n.t(:missing_table, table_name: target_table, scope: :rails_redshift_replicator)
          RailsRedshiftReplicator.logger.error(message) 
          @errors = message
        end
      end

      # Returns records do export
      # @param from_record [Integer] initial record
      #   When the exporter type is identity, the record is an id or equivalent
      #   When the exporter type is timed, the record is the timestamp converted to epoch (date.to_i)
      # @param options [Hash]
      # @option :options [Boolean] :counts_only if should only return record count instead of records
      # @return records [Array<Array>] each entry has its fields returned as an array
      # @note Query cache is disabled to decrease memory usage.
      def records(from_record = nil, option = {})
        @records ||= begin
          ActiveRecord::Base.uncached do
            query_command build_query_sql(from_record, option)
          end
        end
      end

      # Builds the SQL string based on replicable and exporter parameters
      # @return sql [String] sql string to perform the export query
      def build_query_sql(from_record = nil, option = {})
        sql = ""
        sql += "SELECT #{option[:counts_only] ? "count(1) as records_count" : fields_to_sync.join(",")}"
        sql += " FROM #{source_table} WHERE 1=1"
        sql += " AND #{replication_field} > '#{from_record}'" if from_record
        sql += " AND #{replication_field} <= '#{last_record}' OR #{replication_field} IS NULL" if last_record
        sql
      end

      # Returns an instance of a export connection adapter based on ActiveRecord::Base.connection
      # These adapters are required to perform query execution and record retrival in taking advantage of each db driver.
      # @return adapter [#query_command, #write, #last_record_query_command]
      def connection_adapter
        @connection_adapter ||= begin
          adapter_class = if ar_client.adapter_name.in? %w(Mysql2 PostgreSQL SQLite)
            "RailsRedshiftReplicator::Adapters::#{ar_client.adapter_name}".constantize
          else
            RailsRedshiftReplicator::Adapters::Generic
          end
          adapter_class.new ar_client
        end
      end

      # Performs the query to retrive records to export
      # @param sql [String] sql to execute
      def query_command(sql)
        RailsRedshiftReplicator.logger.debug I18n.t(:executing_query, scope: :rails_redshift_replicator, sql: sql, adapter: connection_adapter.class.name)
        connection_adapter.query_command sql
      end

      # Returns the ActiveRecord connection adapter for the current database
      def ar_client
        @ar_client ||= ActiveRecord::Base.connection
      end

      # Retuns the value of last_record from the most recent complete replication record.
      # @return [Integer, nil] last_record
      # @note Some replication strategies may not use a replication_field(eg: FullExporter), so #from_record will be nil
      def from_record
        return if replication_field.blank?
        last_replication.try(:last_record)
      end

      # Exports results to CSV
      # @return [String] file name
      def export(options = {})
        return if errors.present?
        slices = options[:slices] || RailsRedshiftReplicator.redshift_slices.to_i
        format = options[:format] || RailsRedshiftReplicator.preferred_format
        file_name = file_manager.temp_file_name
        initialize_replication(file_name, format, slices)
        export_start = replication.exporting
        counts = file_manager.write_csv file_name, records(from_record)
        unless counts > 0
          RailsRedshiftReplicator.logger.info I18n.t(:no_new_records, table_name: source_table, scope: :rails_redshift_replicator)
          self.replication = nil
          return
        end
        RailsRedshiftReplicator.logger.info I18n.t(:exporting_results, counts: counts, scope: :rails_redshift_replicator)
        file_manager.split_file file_name, counts
        replication.exported! export_duration: (Time.now-export_start).ceil, record_count: counts
        @file_names = Dir.glob "#{file_manager.local_file(file_name)}*"
      end

      # @param [String] nome do arquivo
      # Uploads file to s3
      # @param [String] file name
      def upload(files = file_names)
        return if errors.present? || files.blank?
        upload_start = replication.uploading!
        replication.gzip? ? file_manager.upload_gzip(files) : file_manager.upload_csv(files)
        replication.uploaded! upload_duration: (Time.now-upload_start).ceil
      end

      def file_manager
        @file_manager ||= RailsRedshiftReplicator::FileManager.new(self)
      end

      # Initialize replication record without saving
      # @return [RailsRedshiftReplicator::Replication]
      def initialize_replication(file_name, format, slices)
        attrs = init_replication_attrs(file_name, format, slices)
        @replication = replication.present? ? replication.assign_attributes(attrs) : RailsRedshiftReplicator::Replication.new(attrs)
      end

      def init_replication_attrs(file_name, format, slices)
        {
          key: file_manager.file_key_in_format(file_name, format),
          last_record: last_record.to_s,
          state: 'exporting',
          replication_type: replication_type,
          source_table: source_table,
          target_table: target_table,
          slices: slices,
          first_record: from_record,
          export_format: format
        }
      end

      # Returns the last replication from a given table
      # @return [RailsRedshiftReplicator::Replication] last replication from a given table
      def last_replication
        @last_replication ||= RailsRedshiftReplicator::Replication.from_table(source_table).last
      end

      # Retuns the last record to export using the replication_field criteria.
      # @note last_record is an Integer and is computed based on the replication field.
      # @return [Integer] content of the 
      def last_record
        return if replication_field.blank?
        @last_record ||= begin
          sql = "SELECT max(#{replication_field}) as _last_record from #{source_table}"
          connection_adapter.last_record_query_command(sql)
        end
      end

      # Schema for the export table on redshift
      # @return [Hash] array of fields per table
      def redshift_schema
        @schema ||= begin
          result = ::RailsRedshiftReplicator.connection.exec("select tablename, \"column\", type from pg_table_def where tablename = '#{target_table}'")
          result.to_a.group_by{ |el| el["tablename"] }
        end
      end

      # Lists fields on redshift table
      # @return [Array<String>] colunas na tabela do Redshift
      def fields_to_sync
        @fields_to_sync ||= begin
          column_defs = redshift_schema[target_table]
          column_defs.blank? ? nil : column_defs.map{ |col_def| col_def['column'] }
        end
      end
    end
  end
end
