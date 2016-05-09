module RailsRedshiftReplicator
  module Tools
    class Analyze
      # Updates the query plan to improve performance.
      # @param table [String, :all] table to analyze or :all
      # see [http://docs.aws.amazon.com/redshift/latest/dg/r_ANALYZE.html]
      def initialize(table = nil)
        @table = (table.blank? || table.to_s == "all") ? nil : table
      end

      def perform
        command = "ANALYZE #{@table};".squish
        RailsRedshiftReplicator.logger.debug(command)
        RailsRedshiftReplicator.connection.exec command
      end
    end
  end
end