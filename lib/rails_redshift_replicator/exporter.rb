require 'rails_redshift_replicator/exporters/base'
require 'rails_redshift_replicator/exporters/identity_replicator'
require 'rails_redshift_replicator/exporters/timed_replicator'
require 'rails_redshift_replicator/exporters/full_replicator'

module RailsRedshiftReplicator
  class Exporter
    # Exports one or multiple tables
    # @example Exporting users and posts
    #   RailsRedshiftReplicator::Exporter.new(:users, :publications).perform
    # @param tables [Array<Symbol>, Array<String>] tables to export or :all to export all eligible tables
    def initialize(*tables)
      @options = tables.last.is_a?(Hash) ? tables.pop : {}
      @tables = tables
    end

    def perform
      to_export = tables_to_export(*@tables)
      replicatables(to_export).each { |name, replicatable| replicatable.export }
    end

    # @retuns [Hash] subset of key pairs of replicatables
    def replicatables(tables)
      RailsRedshiftReplicator.replicatables.select { |k,_| k.to_s.in? tables.map(&:to_s) }
    end

    # Returns tables to export. :all selects all eligible
    # @returns [Array<String>] tables to export
    def tables_to_export(*tables)
      raise StandardError.new(I18n.t(:must_specify_tables, scope: :rails_redshift_replicator)) if tables == []
      tables[0] == :all ? eligible_replicatables : filtered_tables(tables)
    end

    # All replicatable tables registered in RailsRedshiftReplicator
    # eighter from the model or directly.
    # @return [Array<String>] tables
    def eligible_replicatables
      RailsRedshiftReplicator.replicatables.keys.map(&:to_s)
    end

    # Validates the given tables to ensure only the ones among the eligibles will be processed
    # @param intersection [Array<Class>] selected tables
    # @return [Array<Class>] filtered selection
    def filtered_tables(subset)
      subset = subset.map(&:to_s)
      eligible_replicatables & subset
    end
  end
end