require 'fog'
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
      FILENAME_SEPARATOR = "-"
      
      # S3 folder inside replication bucket
      # @return [String] prefix
      def self.prefix
        RailsRedshiftReplicator.s3_bucket_params[:prefix]
      end

      # @return fog_connection [Fog::Storage]
      def self.s3_connection
        ::Fog::Storage.new(provider: 'AWS',
                           aws_access_key_id: RailsRedshiftReplicator.aws_credentials[:key],
                           aws_secret_access_key: RailsRedshiftReplicator.aws_credentials[:secret],
                           region: RailsRedshiftReplicator.s3_bucket_params[:region])
      end

      # Returns pointer to s3 bucket
      # @return bucket [Fog::Storage::AWS::Directory]
      def self.replication_bucket
        @directory ||= s3_connection.directories.get(RailsRedshiftReplicator.s3_bucket_params[:bucket], prefix: prefix)
      end

      # File location on s3
      # @return [String] file location
      def self.s3_file_key(source_table, file)
        "#{prefix}/#{source_table}/#{file}"
      end

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
        file_name = "#{source_table}_#{Time.now.to_i}.csv"
        initialize_replication(file_name, format, slices)
        export_start = replication.exporting
        counts = write_csv file_name
        unless counts > 0
          RailsRedshiftReplicator.logger.info I18n.t(:no_new_records, table_name: source_table, scope: :rails_redshift_replicator)
          self.replication = nil
          return
        end
        RailsRedshiftReplicator.logger.info I18n.t(:exporting_results, counts: counts, scope: :rails_redshift_replicator)
        split_file file_name, row_count_threshold(counts)
        replication.exported! export_duration: (Time.now-export_start).ceil, record_count: counts

        @file_names = Dir.glob "#{local_file(file_name)}*"
      end

      # Writes all results to one file for future splitting.
      # @param file_name [String] name of the local export file
      # @return [Integer] number of records to export.
      def write_csv(file_name)
        line_number = connection_adapter.write(local_file(file_name), records(from_record))
      end

      # Path to the local export file
      # @param name [String] file name
      # @return [String] path to file
      def local_file(name)
        @local_file ||= "#{RailsRedshiftReplicator.local_replication_path}/#{name}"
      end

      # Splits the CSV into a number of files determined by the number of Redshift Slices
      # @note This method requires an executable split and is compliant with Mac and Linux versions of it.
      # @param name [String] file name
      # @param counts [Integer] number of files
      def split_file(name, counts)
        file_name = local_file(name)
        `#{RailsRedshiftReplicator.split_command} -l #{counts} #{file_name} #{file_name}.`
      end

      # Number of lines per file
      # @param counts [Integer] number of records
      # @return [Integer] Number of lines per export file
      def row_count_threshold(counts)
        (counts.to_f/replication.slices).ceil
      end

      # Initialize replication record without saving
      # @return [RailsRedshiftReplicator::Replication]
      def initialize_replication(file_name, format, slices)
        attrs = init_replication_attrs(file_name, format, slices)
        @replication = replication.present? ? replication.assign_attributes(attrs) : RailsRedshiftReplicator::Replication.new(attrs)
      end

      def init_replication_attrs(file_name, format, slices)
        {
          key: file_key_in_format(file_name, format),
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

      # Returns the s3 key to be used
      # @return [String] file key with extension
      def file_key_in_format(file_name, format)
        format == "gzip" ? self.class.s3_file_key(source_table, gzipped(file_name)) : self.class.s3_file_key(source_table, file_name)
      end

      # Rename file to use .gz extension
      # @return [String]
      def gzipped(file)
        file.gsub(".csv", ".gz")
      end

      # Returns the last replication from a given table
      # @return [RailsRedshiftReplicator::Replication] last replication from a given table
      def last_replication
        @last_replication ||= RailsRedshiftReplicator::Replication.from_table(source_table).last
      end

      # @param [String] nome do arquivo
      # Uploads file to s3
      # @param [String] file name
      def upload(files = file_names)
        return if errors.present? || files.blank?
        upload_start = replication.uploading!
        replication.gzip? ? upload_gzip(files) : upload_csv(files)
        replication.uploaded! upload_duration: (Time.now-upload_start).ceil
      end

      # @note Broken
      # @todo fix using bash executable
      def upload_gzip(files)
        without_base = files_without_base(files)
        without_base.each do |file|
          basename = File.basename(file)
          command = "#{RailsRedshiftReplicator.gzip_command} -c #{file} > #{gzipped(file)}"
          RailsRedshiftReplicator.logger.info I18n.t(:gzip_notice, file: file, gzip_file: gzipped(file), command: command, scope: :rails_redshift_replicator)
          `#{command}`
          self.class.replication_bucket.files.create key: self.class.s3_file_key(source_table, gzipped(basename)), body: File.open(gzipped(file))
        end
        files.each { |f| FileUtils.rm f }
        without_base.each { |f| FileUtils.rm gzipped(f) }
      end

      def files_without_base(files)
        files.reject{|f| f.split('.').last.in? %w(gz csv)}
      end

      # Uploads splitted CSVs
      # @param files [Array<String>] array of files paths to upload
      def upload_csv(files)
        files.each do |file|
          basename = File.basename(file)
          next if basename == File.basename(replication.key)
          RailsRedshiftReplicator.logger.info I18n.t(:uploading_notice, file: file, key: self.class.s3_file_key(source_table, basename), scope: :rails_redshift_replicator)
          self.class.replication_bucket.files.create key: self.class.s3_file_key(source_table, basename), body: File.open(file)
        end
        files.each { |f| FileUtils.rm f }
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
