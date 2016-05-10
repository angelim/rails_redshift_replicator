module RailsRedshiftReplicator
  class Deleter
    attr_reader :replicable
    def initialize(replicable)
      @replicable = replicable
    end
    # Deletes ids on source database. This ensures that only the records deleted on target
    # will be discarded on source
    def purge_deleted
      ActiveRecord::Base.connection.execute discard_deleted_statement
    end

    # Deletes records flagged for deletion on redshift
    # @return [true, false] if deletion succeded
    def delete_on_target
      RailsRedshiftReplicator.connection.exec(delete_on_target_statement).result_status == 1
    end

    # Deletes records flagged for deletion on target and then delete the queue from source
    def handle_delete_propagation
      if replicable.tracking_deleted && has_deleted_ids?
        RailsRedshiftReplicator.logger.info propagation_message(:propagating_deletes)
        delete_on_target ? purge_deleted : RailsRedshiftReplicator.logger.error(propagation_message(:delete_propagation_error))
      end
    end

    def propagation_message(key)
      I18n.t(key, table_name: replicable.source_table, count: deleted_ids.count, scope: :rails_redshift_replicator)
    end

    # Retrives ids of objects enqueued for deletion for the replication source table
    # @example 
    # deleted_ids #=> "1,2,3,4,5"
    # @return [String] list of ids enqueued to delete on target
    def deleted_ids
      sql = <<-DR.squish
        SELECT object_id
        FROM rails_redshift_replicator_deleted_ids
        WHERE source_table = '#{replicable.source_table}'
      DR
      ActiveRecord::Base.connection.execute(sql).map{ |r| r['object_id'] }
    end

    # If has objects to delete on target
    # @return [true, false]
    def has_deleted_ids?
      deleted_ids.present?
    end

    # Builds the statement to perform a deletion on source
    # @return [String] SQL statement
    def discard_deleted_statement
      sql = <<-DD.squish
        DELETE FROM rails_redshift_replicator_deleted_ids
        WHERE source_table = '#{replicable.source_table}'
        AND object_id IN(#{deleted_ids.join(",")})
      DD
    end

    def delete_on_target_statement
      sql = <<-DD.squish
        DELETE FROM #{replicable.target_table}
        WHERE id IN(#{deleted_ids.join(",")})
      DD
    end
  end
end