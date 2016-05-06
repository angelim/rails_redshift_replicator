module RailsRedshiftReplicator
  module Exporters
    class FullReplicator < Base
      def self.replication_field
        nil
      end
    end
  end
end