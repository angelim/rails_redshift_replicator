module RailsRedshiftReplicator
  module Adapters
    class SQLite < Generic
      # @param conn [ActiveRecord::ConnectionAdapter]
      # @param sql [String] sql to execute
      def query_command(sql)
        connection.exec_query sql
      end

      def write(file_path, query_result)
        line_number = 0
        CSV.open(file_path, "w") do |csv|
          query_result.each do |row|
            csv << row.map{ |_,field| field.is_a?(String) ? field.gsub("\n", " ") : field }
            line_number+=1
          end
        end
        line_number
      end
    end
  end
end