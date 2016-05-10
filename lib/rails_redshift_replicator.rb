require 'hair_trigger'
require 'active_support'
require "rails_redshift_replicator/engine"
require 'rails_redshift_replicator/model/extension'
require 'rails_redshift_replicator/model/hair_trigger_extension'
require 'rails_redshift_replicator/replicable'
require 'rails_redshift_replicator/deleter'

require 'rails_redshift_replicator/exporters/base'
require 'rails_redshift_replicator/exporters/identity_replicator'
require 'rails_redshift_replicator/exporters/timed_replicator'
require 'rails_redshift_replicator/exporters/full_replicator'

require 'rails_redshift_replicator/importers/base'
require 'rails_redshift_replicator/importers/identity_replicator'
require 'rails_redshift_replicator/importers/timed_replicator'
require 'rails_redshift_replicator/importers/full_replicator'

require 'rails_redshift_replicator/tools/analyze'
require 'rails_redshift_replicator/tools/vacuum'


module RailsRedshiftReplicator
  mattr_accessor :replicables, :logger, :redshift_connection_params, :aws_credentials, :s3_bucket_params,
                 :redshift_slices, :local_replication_path, :debug_mode, :history_cap, :max_copy_errors,
                 :split_command, :gzip_command, :preferred_format, :max_retries, :enable_delete_tracking

  class << self

    # @note Useful for testing
    def define_defaults
      @@replicables = {}.with_indifferent_access
      @@logger = Logger.new(STDOUT).tap{ |l| l.level = Logger::WARN }
      
      # Connection parameters for Redshift. Defaults to environment variables.
      @@redshift_connection_params = {
        host: ENV['RRR_REDSHIFT_HOST'],
        dbname: ENV['RRR_REDSHIFT_DATABASE'],
        port: ENV['RRR_REDSHIFT_PORT'],
        user: ENV['RRR_REDSHIFT_USER'],
        password: ENV['RRR_REDSHIFT_PASSWORD']
      }

      # AWS S3 Replication bucket credentials. Defaults to environment variables.
      @@aws_credentials = {
        key: ENV['RRR_AWS_ACCESS_KEY_ID'],
        secret: ENV['RRR_AWS_SECRET_ACCESS_KEY']
      }

      # AWS S3 replication bucket parameters.
      # region defaults to environment variable or US East (N. Virginia)
      # bucket defaults to environment variable
      @@s3_bucket_params = {
        region: (ENV['RRR_REPLICATION_REGION'] || 'us-east-1'),
        bucket: ENV['RRR_REPLICATION_BUCKET'],
        prefix: ENV['RRR_REPLICATION_PREFIX']
      }

      # Number of slices available on Redshift cluster. Used to split export files. Defaults to 1.
      # see [http://docs.aws.amazon.com/redshift/latest/dg/t_splitting-data-files.html]
      @@redshift_slices = 1

      # Folder to store temporary replication files until the S3 upload. Defaults to /tmp
      @@local_replication_path = '/tmp'

      # Enable debug mode to output messages to STDOUT. Default to false
      @@debug_mode = false

      # Defines how many replication records are kept in history. Default to nil keeping full history.
      @@history_cap = nil

      # Defines how many replication records are kept in history. Default to nil keeping full history.
      @@max_copy_errors = 0

      # Command or path to executable that splits files
      @@split_command = 'split'

      # Command or path to executable that compresses files to gzip
      @@gzip_command = 'gzip'

      # Preferred format for export file
      @@preferred_format = 'csv'

      # Maximum number of retries for a replication before cancelling and starting another
      @@max_retries = nil

      # If deletes should be tracked and propagated to redshift
      @@enable_delete_tracking = false

      return nil
    end
    alias redefine_defaults define_defaults

    def debug_mode=(value)
      logger.level = value == true ? Logger::DEBUG : Logger::WARN
      @@debug_mode = value
    end

    # @return [RedshiftReplicator]
    def setup
      yield self
    end

    def add_replicable(hash)
      logger.debug I18n.t(:replicable_added, table_name: hash.keys.first, scope: :rails_redshift_replicator) 
      RailsRedshiftReplicator.replicables.merge! hash
    end

    def reload_replicables
      replicables = {}
      replicables.each do |name, replicable|
        add_replicable(name => RailsRedshiftReplicator::Replicable.new(replicable.replication_type, replicable.options))
      end
    end

    # Performs full replication (export + import)
    # @param models [Array<Symbol>, Argument list] activerecord models to export or :all
    # @example Replicate user and post models.
    #   RedshiftReplicator.replicate(:user, :publication)
    # @example Replicate all models
    #   RedshiftReplicator.replicate(:all)
    def replicate(*tables)
      check_args(tables)
      replicable_definitions(tables_to_perform(tables)).each do |_, replicable|
        replication = replicable.export
        replicable.import
      end
    end

    # @see .replicate
    def export(*tables)
      check_args(tables)
      replicable_definitions(tables_to_perform(tables)).each { |_, replicable| replicable.export }
    end

    # @see .replicate
    def import(*tables)
      check_args(tables)
      replicable_definitions(tables_to_perform(tables)).each { |_, replicable| replicable.import }
    end

    def check_args(tables)
      if tables == []
        error_message = I18n.t(:must_specify_tables, scope: :rails_redshift_replicator)
        logger.error error_message
        raise StandardError.new(error_message)
      end
    end

    def vacuum(*args)
      Tools::Vacuum.new(*args).perform
    end

    def analyze(*args)
      Tools::Analyze.new(*args).perform
    end

    # Lists exporters names
    def base_exporter_types
      [
        'identity_replicator',
        'timed_replicator',
        'full_replicator'
      ]
    end

    # All replicable tables registered in RailsRedshiftReplicator
    # eighter from the model or directly.
    # @return [Array<String>] tables
    def replicable_tables
      RailsRedshiftReplicator.replicables.keys.map(&:to_s)
    end

    def replicable_target_tables
      RailsRedshiftReplicator.replicables.map{ |k,v| v[:target_table] }
    end

    # @retuns [Hash] subset of key pairs of replicables
    def replicable_definitions(tables)
      RailsRedshiftReplicator.replicables.select { |k,_| k.to_s.in? tables.map(&:to_s) }
    end

    # Returns tables to export. :all selects all eligible
    # @returns [Array<String>] tables to export
    def tables_to_perform(tables)
      tables = Array(tables).map(&:to_s)
      if tables[0] == 'all'
        replicable_tables
      else
        (replicable_tables & tables).tap do |selected|
          warn_if_unreplicable tables-selected
        end
      end
    end

    def warn_if_unreplicable(tables)
      tables.each { |table| logger.warn I18n.t(:table_not_replicable, table_name: table, scope: :rails_redshift_replicator) }
    end

    # Redshift connection
    # @return [PG::Connection]
    def connection
      @redshift ||= PG.connect(redshift_connection_params)
    end
  end
end
RailsRedshiftReplicator.define_defaults