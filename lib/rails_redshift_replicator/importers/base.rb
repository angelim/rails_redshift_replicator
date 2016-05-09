require 'pg'
module RailsRedshiftReplicator
  module Importers
    class Base
      attr_accessor :replication
      def initialize(replication)
        return if replication.blank?
        @replication = replication
      end

      def import
        raise NotImplementedError
      end

      # Runs Redshift COPY command to import data from S3
      # (http://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html)
      # @param [String] table name
      # @param options [Hash]
      # @option options [Boolean] :mark_as_imported If record should be flagged as imported
      # @option options [Boolean] :noload If true, data will be validated but not imported
      def copy(table_name = replication.target_table, options = {})
        begin
          RailsRedshiftReplicator.logger.info I18n.t(:importing_file, file: import_file, target_table: table_name, scope: :rails_redshift_replicator)
          result = ::RailsRedshiftReplicator.connection.exec copy_statement(table_name, options)
          replication.imported! if result.result_status == 1 && options[:mark_as_imported]
        rescue => e
          drop_table(table_name) if options[:can_drop_target_on_error]
          if e.message.index("stl_load_errors")
            get_redshift_error
            notify_error
          else
            replication.update_attribute :last_error, e.exception.inspect
          end
        end
      end

      # Builds the copy statement
      # @param (see #copy)
      # @return [String] sql statement to run
      def copy_statement(table_name, options = {})
        format_options = replication.csv? ? "CSV" : "GZIP delimiter ',' escape removequotes"
        sql = <<-CS
          COPY #{table_name} from '#{import_file}' #{"NOLOAD" if options[:noload]}
          REGION '#{RailsRedshiftReplicator.s3_bucket_params[:region]}'
          credentials 'aws_access_key_id=#{RailsRedshiftReplicator.aws_credentials[:key]};aws_secret_access_key=#{RailsRedshiftReplicator.aws_credentials[:secret]}'
          maxerror #{RailsRedshiftReplicator.max_copy_errors} acceptinvchars STATUPDATE true #{format_options}
        CS
        sql.squish
      end

      # @return [String] location of import files on s3
      def import_file
        "s3://#{RailsRedshiftReplicator.s3_bucket_params[:bucket]}/#{replication.key}"
      end

      # Retrieves the last copy error for a given file on redshift
      def get_redshift_error
        sql = <<-RE.squish
          SELECT filename, line_number, colname, type, raw_field_value, raw_line, err_reason
          FROM STL_LOAD_ERRORS
          WHERE filename like '%#{import_file}%'
          ORDER BY starttime desc
          LIMIT 1
        RE
        result = ::RailsRedshiftReplicator.connection.exec(sql).entries
        error = result.first.map{ |k, v| [k, v.strip].join('=>') }.join(";")
        replication.update_attribute :last_error, error
      end

      # TODO
      def notify_error
      end

      # Creates a temporary table on redshift
      def create_temp_table
        ::RailsRedshiftReplicator.connection.exec "CREATE TEMP TABLE #{temporary_table_name} (LIKE #{replication.target_table})"
      end

      # Creates a permanent table for later renaming
      def create_side_table
        ::RailsRedshiftReplicator.connection.exec "CREATE TABLE #{temporary_table_name} (LIKE #{replication.target_table})"
      end

      # Runs a merge or replace operation on a redshift table
      # The table is replaced on a FullReplicator strategy
      # The table is merged on a TimedReplicator strategy
      # @param :mode [Symbol] the operation type
      def merge_or_replace(mode:)
        target  = replication.target_table
        stage = temporary_table_name
        sql = send("#{mode}_statement", target, stage)
        ::RailsRedshiftReplicator.connection.exec sql
      end

      # Builds the merge sql statement.
      # At first, it deletes the matching records from the target and temporary tables on the target table.
      # After it imports everything from the temporary table into the target table.
      # @param target [String] 
      # @param stage [String] temporary table
      # @return [String] Sql Statement
      # (http://docs.aws.amazon.com/redshift/latest/dg/merge-replacing-existing-rows.html)
      def merge_statement(target, stage)
        <<-SQLMERGE
          begin transaction;

          delete from #{target}
          using #{stage}
          where #{target}.id = #{stage}.id;
          insert into #{target}
          select * from #{stage};

          end transaction;
        SQLMERGE
      end

      # Builds the replace sql statement.
      # @param (see #merge_statement)
      # @return (see #merge_statement)
      # (http://docs.aws.amazon.com/redshift/latest/dg/performing-a-deep-copy.html)
      def replace_statement(target, stage)
        <<-SQLREPLACE
          begin transaction;
          drop table #{target};
          alter table #{stage} rename to #{target};
          end transaction;
        SQLREPLACE
      end

      # Deletes the temporary table
      # @param table_name [String]
      def drop_table(table_name = temporary_table_name)
        ::RailsRedshiftReplicator.connection.exec "drop table if exists #{table_name}"
      end

      # Returns a random name for a temporary table
      # @return [String] table name
      def temporary_table_name
        @temp_table ||= "temp_#{replication.target_table}_#{Time.now.to_i}"
      end
    end
  end
end
