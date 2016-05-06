module RailsRedshiftReplicator
  module Exporters
    class TimedReplicator < Base
      def self.replication_field
        "updated_at"
      end
    end
  end
end