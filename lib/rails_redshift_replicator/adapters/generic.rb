module RailsRedshiftReplicator
  module Adapters
    class Generic
      def initialize(ar_client)
        @ar_client = ar_client
      end

      # Returns the AR connection
      def connection
        @connection ||= @ar_client
      end
      
      # Executes query using AR Adapter
      # @param sql [String] sql to execute
      def query_command(sql)
        connection.query sql
      end

      # @return [String] id or timestamp
      def last_record_query_command(sql)
        connection.exec_query(sql).first['_last_record']
      end

      # Writes query results to a file
      # @param file_path [String] path to output
      # @param query_result [#each] Resultset from the query_command
      # @return [Integer] number of records
      def write(file_path, query_result)
        line_number = 0
        CSV.open(file_path, "w") do |csv|
          query_result.each do |row|
            csv << row.map{ |field| field.is_a?(String) ? field.gsub("\n", " ") : field }
            line_number+=1
          end
        end
        line_number
      end
    end
  end
end