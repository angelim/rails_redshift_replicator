module RailsRedshiftReplicator
  module Adapters
    class PostgreSQL < Generic
      # @param conn [ActiveRecord::ConnectionAdapter]
      # @param sql [String] sql to execute
      def query_command(sql)
        connection.send_query(sql)
        connection.set_single_row_mode
      end

      def connection
        @connection ||= @ar_client.instance_variable_get("@connection")
      end

      def last_record_query_command(sql)
        @ar_client.exec_query(sql).first['_last_record']
      end

      def write(file_path, query_result)
        line_number = 0
        CSV.open(file_path, "w") do |csv|
          query_result.get_result.stream_each do |row|
            csv << row.map{ |_,field| field.is_a?(String) ? field.gsub("\n", " ") : field }
            line_number+=1
          end
        end
        line_number
      end
    end
  end
end
