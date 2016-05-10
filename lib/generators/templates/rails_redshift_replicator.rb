RailsRedshiftReplicator.setup do |config|
  # RRR already provides a logger pointing to STDOUT, but you can point it to your own logger.
  # Just be sure to make it inherit from RailsRedshiftReplicator::RLogger or you will loose
  # the notifications feature.
  # config.logger = MyLogger.new

  # Connection parameters for Redshift. Defaults to environment variables.
  config.redshift_connection_params = {
    host: ENV['RRR_REDSHIFT_HOST'],
    dbname: ENV['RRR_REDSHIFT_DATABASE'],
    port: ENV['RRR_REDSHIFT_PORT'],
    user: ENV['RRR_REDSHIFT_USER'],
    password: ENV['RRR_REDSHIFT_PASSWORD']
  }

  # AWS S3 Replication bucket credentials. Defaults to environment variables.
  config.aws_credentials = {
    key: ENV['RRR_AWS_ACCESS_KEY_ID'],
    secret: ENV['RRR_AWS_SECRET_ACCESS_KEY']
  }

  # AWS S3 replication bucket parameters.
  # region defaults to environment variable or US East (N. Virginia)
  # bucket defaults to environment variable
  config.s3_bucket_params = {
    region: (ENV['RRR_REPLICATION_REGION'] || 'us-east-1'),
    bucket: ENV['RRR_REPLICATION_BUCKET'],
    prefix: ENV['RRR_REPLICATION_PREFIX']
  }

  # Number of slices available on Redshift cluster. Used to split export files. Defaults to 1.
  # see [http://docs.aws.amazon.com/redshift/latest/dg/t_splitting-data-files.html]
  config.redshift_slices = 1

  # Folder to store temporary replication files until the S3 upload. Defaults to /tmp
  config.local_replication_path = '/tmp'

  # Command or path to executable that splits files
  config.split_command = 'split'

  # Command or path to executable that compresses files to gzip
  config.gzip_command = 'gzip'

  # Enable debug mode to output messages to STDOUT. Default to false
  config.debug_mode = false

  # Defines how many replication records are kept in history. Default to nil keeping full history.
  config.history_cap = nil

  # Defines how many errors are allowed to happen when importing into Redshfit
  # see [http://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-load.html#copy-maxerror]
  config.max_copy_errors = 0

  # Preferred format for export file
  config.preferred_format = 'csv'

  # Maximum number of retries for a replication before cancelling and starting another
  config.max_retries = 5

  # If deletes should be tracked and propagated to redshift
  # Take a look at the "A word on tracking deletions" section
  config.enable_delete_tracking = false
end