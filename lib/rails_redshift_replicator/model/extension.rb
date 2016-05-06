module RailsRedshiftReplicator
  module Model
    module Extension
      def self.included(base)
        base.send :extend, ClassMethods
      end
      
      module ClassMethods
        def has_redshift_replication(replication_type, options = {})
          cattr_accessor :rails_redshift_replicator_replicatable
          replication_type = replication_type.to_s
          raise I18n.t(:replication_type_not_supported,
                       replication_type: replication_type,
                       types: RailsRedshiftReplicator.base_exporter_types.join(","),
                       scope: :exception_messages) unless replication_type.in? RailsRedshiftReplicator.base_exporter_types
          extend Actions
          options[:source_table] ||= self.table_name
          self.rails_redshift_replicator_replicatable = RailsRedshiftReplicator::Replicatable.new(replication_type, options)
          RailsRedshiftReplicator.add_replicatable({ options[:source_table] => rails_redshift_replicator_replicatable })
        end
      end
    end
    module Actions
      def rails_redshift_replicator_export
        rails_redshift_replicator_replicatable.export
      end
      def rails_redshift_replicator_import
        rails_redshift_replicator_replicatable.import
      end

      def rails_redshift_replicator_replicate
        rails_redshift_replicator_replicatable.replicate
      end
    end
  end
end
ActiveRecord::Base.send :include, RailsRedshiftReplicator::Model::Extension