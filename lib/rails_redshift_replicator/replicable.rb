module RailsRedshiftReplicator
  class Replicable
    include HairTrigger::Base
    attr_reader :source_table, :target_table, :replication_field, :replication_type, :tracking_deleted, :options
    # alias for hairtrigger
    alias table_name source_table

    # @param replication_type [String, Symbol]
    # @param options [Hash] Replication options
    # @option options [String, Symbol] :source_table name of the source table to replicate
    # @option options [String, Symbol] :target_table name of the target table on redshift
    # @option options [String, Symbol] :replication_field name of the replication field
    def initialize(replication_type, options = {})
      @replication_type = replication_type
      @options = options
      @source_table = options[:source_table].to_s
      @target_table = (options[:target_table] || source_table).to_s
      replication_field = options[:replication_field] || exporter_class.replication_field
      @replication_field = replication_field && replication_field.to_s
      @tracking_deleted = delete_tracking_enabled?
      if tracking_deleted
        trigger.after(:delete) do
          "INSERT INTO rails_redshift_replicator_deleted_ids(source_table, object_id) VALUES('#{source_table}', OLD.id);"
        end
      end
    end

    def reset_last_record
      last_imported_replication = RailsRedshiftReplicator::Replication.from_table(source_table).with_state(:imported).last
      last_imported_replication && last_imported_replication.update_attribute(:last_record, nil)
    end

    def delete_tracking_enabled?
      RailsRedshiftReplicator.enable_delete_tracking &&
      (options[:enable_delete_tracking].blank? || options[:enable_delete_tracking]) &&
      table_supports_tracking?
    end

    def table_supports_tracking?
      ActiveRecord::Base.connection.columns(source_table).map(&:name).include? "id"
    end

    def replicate
      export
      import
    end

    def import
      @last_replication = nil
      if last_replication.present?
        if last_replication.uploaded?
          perform_import
        elsif last_replication.imported?
          RailsRedshiftReplicator.logger.info I18n.t(:nothing_to_import, table_name: source_table, scope: :rails_redshift_replicator)
        elsif max_retries_reached?
          last_replication.cancel!
        else
          resume_replication
        end
      else
        RailsRedshiftReplicator.logger.info I18n.t(:nothing_to_import, table_name: source_table, scope: :rails_redshift_replicator)
      end
    end

    def export
      @last_replication = nil
      if last_replication.blank? || (last_replication && last_replication.imported?)
        perform_export
      else
        if max_retries_reached?
          last_replication.cancel!
          perform_export
        else
          resume_replication
        end
      end
    end

    def perform_export(replication = nil)
      exporter_class.new(self, replication).export_and_upload
    end

    def perform_import
      importer_class.new(last_replication).import
    end

    def max_retries_reached?
      if RailsRedshiftReplicator.max_retries && (RailsRedshiftReplicator.max_retries == last_replication.retries)
        RailsRedshiftReplicator.logger.warn I18n.t(:max_retries_reached, id: last_replication, table_name: source_table, scope: :rails_redshift_replicator)
        return true
      else
        false
      end
    end

    def last_replication
      @last_replication ||= RailsRedshiftReplicator::Replication.from_table(source_table).last
    end

    def resume_replication
      last_replication.increment! :retries, 1
      if last_replication.state.in? %w(enqueued exporting exported uploading)
        log_resuming('export')
        perform_export(last_replication)
      else
        log_resuming('import')
        perform_import
      end
    end

    def log_resuming(action)
      RailsRedshiftReplicator.logger.info I18n.t(:resuming_replication, table_name: source_table, action: action, state: last_replication.state, scope: :rails_redshift_replicator)
    end

    def vacuum
      RailsRedshiftReplicator.vacuum(target_table)
    end

    def analyze
      RailsRedshiftReplicator.analyze(target_table)
    end

    def deleter
      RailsRedshiftReplicator::Deleter.new(self)
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