module RailsRedshiftReplicator
  module Adapters
    class Mysql2 < Generic
      # @param conn [ActiveRecord::ConnectionAdapter]
      # @param sql [String] sql to execute
      # Pode ser usado caso a exportação esteja trazendo problemas de estouro de memória.
      # Apesar de o query cache estar desabilitado, o Mysql2::Result continua sendo carregado com o resultado completo da query.
      # Usando o Mysql2::Client diretamente, podemos fazer uma consulta no modo Stream, baixando informações do MySql sob demanda.
      # O Mysql2Adapter não contempla o uso dessa opção.
      # (http://dev.mysql.com/doc/refman/5.0/en/mysql-use-result.html)
      # (https://github.com/brianmario/mysql2#streaming)
      # @example
      #   result = mysql2_client.query("select * from views", stream: true)
      #   result.count #=> 0
      #   result.to_a.count #=> 1000

      def query_command(sql)
        connection.query(sql, stream: true)
      end

      def last_record_query_command(sql)
        connection.query(sql, cast: false).first[0] rescue nil
      end

      def connection
        @connection ||= @ar_client.instance_variable_get("@connection")
      end
    end
  end
end
