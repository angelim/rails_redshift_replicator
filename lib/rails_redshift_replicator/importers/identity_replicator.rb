module RailsRedshiftReplicator
  module Importers
    class IdentityReplicator < Base
      def import
        import_start = replication.importing!
        copy replication.target_table, mark_as_imported: true
        replication.update_attributes import_duration: (Time.now-import_start).ceil
      end
    end
  end
end