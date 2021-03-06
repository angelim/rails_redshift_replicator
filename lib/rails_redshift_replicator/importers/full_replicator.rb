module RailsRedshiftReplicator
  module Importers
    class FullReplicator < Base
      def import
        import_start = replication.importing!
        create_side_table
        copy temporary_table_name, mark_as_imported: false, can_drop_target_on_error: true
        return if replication.error?
        merge_or_replace(mode: :replace)
        replication.clear_errors!
        replication.imported! import_duration: (Time.now-import_start).ceil
        evaluate_history_cap
        file_manager.delete if RailsRedshiftReplicator.delete_s3_file_after_import
      end
    end
  end
end