require 'aws-sdk'
module RailsRedshiftReplicator
  class FileManager
    attr_reader :exporter

    def s3_client
      @client ||= Aws::S3::Client.new(
        region: RailsRedshiftReplicator.s3_bucket_params[:region],
        access_key_id: RailsRedshiftReplicator.aws_credentials[:key],
        secret_access_key: RailsRedshiftReplicator.aws_credentials[:secret]
      )
    end

    # File location on s3
    # @return [String] file location
    def self.s3_file_key(source_table, file)
      File.join RailsRedshiftReplicator.s3_bucket_params[:prefix], source_table, file
    end

    def initialize(exporter = nil)
      @exporter = exporter
    end

    def temp_file_name
      "#{exporter.source_table}_#{Time.now.to_i}.csv"
    end

    # Writes all results to one file for future splitting.
    # @param file_name [String] name of the local export file
    # @return [Integer] number of records to export.
    def write_csv(file_name, records)
      line_number = exporter.connection_adapter.write(local_file(file_name), records)
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
    def split_file(name, record_count)
      counts = row_count_threshold(record_count)
      file_name = local_file(name)
      `#{RailsRedshiftReplicator.split_command} -l #{counts} #{file_name} #{file_name}.`
    end

    # Number of lines per file
    # @param counts [Integer] number of records
    # @return [Integer] Number of lines per export file
    def row_count_threshold(counts)
      (counts.to_f/exporter.replication.slices).ceil
    end


    # Returns the s3 key to be used
    # @return [String] file key with extension
    def file_key_in_format(file_name, format)
      if format == "gzip"
        self.class.s3_file_key exporter.source_table, gzipped(file_name)
      else
        self.class.s3_file_key exporter.source_table, file_name
      end
    end

    # Rename file to use .gz extension
    # @return [String]
    def gzipped(file)
      file.gsub(".csv", ".gz")
    end

    def upload_gzip(files)
      without_base = files_without_base(files)
      without_base.each do |file|
        basename = File.basename(file)
        command = "#{RailsRedshiftReplicator.gzip_command} -c #{file} > #{gzipped(file)}"
        RailsRedshiftReplicator.logger.info I18n.t(:gzip_notice, file: file, gzip_file: gzipped(file), command: command, scope: :rails_redshift_replicator)
        `#{command}`
        s3_client.put_object(
          key: self.class.s3_file_key(exporter.source_table, gzipped(basename)),
          body: File.open(gzipped(file)),
          bucket: RailsRedshiftReplicator.s3_bucket_params[:bucket]
        )
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
        next if basename == File.basename(exporter.replication.key)
        RailsRedshiftReplicator.logger.info I18n.t(:uploading_notice,
                                                   file: file,
                                                   key: self.class.s3_file_key(exporter.source_table, basename),
                                                   scope: :rails_redshift_replicator)
        s3_client.put_object(
          key: self.class.s3_file_key(exporter.source_table, basename),
          body: File.open(file),
          bucket: RailsRedshiftReplicator.s3_bucket_params[:bucket]
        )
      end
      files.each { |f| FileUtils.rm f }
    end
  end
end