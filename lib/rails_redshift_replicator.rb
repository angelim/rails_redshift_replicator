require "rails_redshift_replicator/engine"
require 'active_support'
require 'rails_redshift_replicator/exporter'
require 'rails_redshift_replicator/importer'
require 'rails_redshift_replicator/model/extension'
require 'rails_redshift_replicator/replicatable'

module RailsRedshiftReplicator
  mattr_accessor :replicatables
  @@replicatables = {}.with_indifferent_access

  # Connection parameters for Redshift. Defaults to environment variables.
  mattr_accessor :redshift_connection_params
  @@redshift_connection_params = {
    host: ENV['RRR_REDSHIFT_HOST'],
    dbname: ENV['RRR_REDSHIFT_DATABASE'],
    port: ENV['RRR_REDSHIFT_PORT'],
    user: ENV['RRR_REDSHIFT_USER'],
    password: ENV['RRR_REDSHIFT_PASSWORD']
  }

  # AWS S3 Replication bucket credentials. Defaults to environment variables.
  mattr_accessor :aws_credentials
  @@aws_credentials = {
    key: ENV['RRR_AWS_ACCESS_KEY_ID'],
    secret: ENV['RRR_AWS_SECRET_ACCESS_KEY']
  }

  # AWS S3 replication bucket parameters.
  # region defaults to environment variable or US East (N. Virginia)
  # bucket defaults to environment variable
  mattr_accessor :s3_bucket_params
  @@s3_bucket_params = {
    region: (ENV['RRR_REPLICATION_REGION'] || 'us-east-1'),
    bucket: ENV['RRR_REPLICATION_BUCKET'],
    prefix: ENV['RRR_REPLICATION_PREFIX']
  }

  # Number of slices available on Redshift cluster. Used to split export files. Defaults to 1.
  # see [http://docs.aws.amazon.com/redshift/latest/dg/t_splitting-data-files.html]
  mattr_accessor :redshift_slices
  @@redshift_slices = 1

  # Folder to store temporary replication files until the S3 upload. Defaults to /tmp
  mattr_accessor :local_replication_path
  @@local_replication_path = '/tmp'

  # Enable debug mode to output messages to STDOUT. Default to false
  mattr_accessor :debug_mode
  @@debug_mode = false

  # Defines how many replication records are kept in history. Default to nil keeping full history.
  mattr_accessor :history_cap
  @@history_cap = nil

  LOGGER = Logger.new(STDOUT)

  class << self
    # @return [RedshiftReplicator]
    def setup
      yield self
    end

    def add_replicatable(hash)
      RailsRedshiftReplicator.replicatables.merge! hash
    end

    # Performs full replication (export + import)
    # @param models [Array<Symbol>, Argument list] activerecord models to export or :all
    # @example Replicate user and post models.
    #   RedshiftReplicator.replicate(:user, :publication)
    # @example Replicate all models
    #   RedshiftReplicator.replicate(:all)
    def replicate(*args)
      export(*args)
      import(*args)
    end

    # @see .replicate
    def export(*args)
      Exporters::Base.export *args
    end

    # @see .replicate
    def import(*args)
      Importers::Base.import *args
    end

    def vacuum(*args)
      Tools::Vacuum.new *args
    end

    def analyze(*args)
      Tools::Analyze.new *args
    end

    # Lists exporters names
    def base_exporter_types
      [
        'identity_replicator',
        'timed_replicator',
        'full_replicator'
      ]
    end

    # Redshift connection
    # @return [PG::Connection]
    def connection
      @redshift ||= PG.connect(redshift_connection_params)
    end
  end
end
