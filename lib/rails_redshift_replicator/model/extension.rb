module RailsRedshiftReplicator
  module Model
    module Extension
      def self.included(base)
        base.send :extend, ClassMethods
      end
      
      module ClassMethods
        def has_redshift_replication(replication_type, options = {})
          cattr_accessor :rails_redshift_replicator_replicable
          replication_type = replication_type.to_s
          raise I18n.t(:replication_type_not_supported,
                       replication_type: replication_type,
                       types: RailsRedshiftReplicator.base_exporter_types.join(","),
                       scope: :exception_messages) unless replication_type.in? RailsRedshiftReplicator.base_exporter_types
          extend Actions
          options[:source_table] ||= self.table_name
          self.rails_redshift_replicator_replicable = RailsRedshiftReplicator::Replicable.new(replication_type, options)
          RailsRedshiftReplicator.add_replicable({ options[:source_table] => rails_redshift_replicator_replicable })
        end
      end
    end
    module Actions
      def rrr_export
        rails_redshift_replicator_replicable.export
      end
      def rrr_import
        rails_redshift_replicator_replicable.import
      end
      def rrr_replicate
        rails_redshift_replicator_replicable.replicate
      end
      def rrr_vaccum
        rails_redshift_replicator_replicable.vaccum
      end
      def rrr_analyze
        rails_redshift_replicator_replicable.analyze
      end
    end
  end
end
ActiveRecord::Base.send :include, RailsRedshiftReplicator::Model::Extension