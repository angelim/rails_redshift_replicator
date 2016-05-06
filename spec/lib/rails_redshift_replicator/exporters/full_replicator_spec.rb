require 'spec_helper'

describe RailsRedshiftReplicator::Exporter::FullReplicator, type: :redshift_replicator, replicator: true do
  let(:user_replicatable) { RailsRedshiftReplicator::Replicatable.new(:full_replicator, source_table: :tags_users) }
  describe "#replication_field", :focus do
    it "returns the replicatable replication_field" do
      expect(RailsRedshiftReplicator::Exporter::FullReplicator.new(user_replicatable).replication_field).to be_nil
    end
  end
end