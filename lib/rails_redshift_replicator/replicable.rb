module RailsRedshiftReplicator
  class Replicable
    attr_reader :source_table, :target_table, :replication_field, :replication_type

    # @param replication_type [String, Symbol]
    # @param options [Hash] Replication options
    # @option options [String, Symbol] :source_table name of the source table to replicate
    # @option options [String, Symbol] :target_table name of the target table on redshift
    # @option options [String, Symbol] :replication_field name of the replication field
    def initialize(replication_type, options = {})
      @replication_type = replication_type
      @source_table = options[:source_table].to_s
      @target_table = (options[:target_table] || source_table).to_s
      replication_field = options[:replication_field] || exporter_class.replication_field
      @replication_field = replication_field && replication_field.to_s
    end

    def replicate
      export
      import
    end

    def import(replication = last_replication)
      if replication.present?
        importer_class.new(replication).import
      else
        RailsRedshiftReplicator.logger.info I18n.t(:nothing_to_import, table_name: source_table, scope: :rails_redshift_replicator)
      end
    end

    def last_replication
      RailsRedshiftReplicator::Replication.from_table(source_table).uploaded.last
    end

    def vacuum
      RailsRedshiftReplicator.vacuum(target_table)
    end

    def analyze
      RailsRedshiftReplicator.analyze(target_table)
    end

    def export
      exporter_class.new(self).export_and_upload
    end

    def exporter_class
      @exporter_class ||= begin
        "RailsRedshiftReplicator::Exporters::#{replication_type.to_s.classify}".constantize
      rescue
        raise StandardError.new I18n.t(:missing_replicator_type, scope: :rails_redshift_replicator)
      end
    end

    def importer_class
      @importer_class ||= begin
        "RailsRedshiftReplicator::Importers::#{replication_type.to_s.classify}".constantize
      rescue
        raise StandardError.new I18n.t(:missing_replicator_type, scope: :rails_redshift_replicator)
      end
    end
  end
end