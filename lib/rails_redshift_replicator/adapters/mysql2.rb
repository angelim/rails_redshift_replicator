module RailsRedshiftReplicator
  module Adapters
    class Mysql2 < Generic

      # Executes query in stream mode to optimize memory usage, using Mysql2 driver.
      # @param sql [String] sql to execute
      def query_command(sql)
        connection.query(sql, stream: true)
      end

      # @see RailsRedshiftReplicator::Adapters::Generic#last_record_query_command
      def last_record_query_command(sql)
        connection.query(sql, cast: false).first[0] rescue nil
      end

      # Returns mysql2 driver so that we may perform query with the stream option
      def connection
        @connection ||= @ar_client.instance_variable_get("@connection")
      end
    end
  end
end
