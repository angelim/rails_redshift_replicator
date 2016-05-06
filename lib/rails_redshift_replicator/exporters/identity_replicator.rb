module RailsRedshiftReplicator
  module Exporters
    class IdentityReplicator < Base
      def self.replication_field
        "id"
      end
    end
  end
end