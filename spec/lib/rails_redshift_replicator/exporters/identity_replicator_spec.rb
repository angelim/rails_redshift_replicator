require 'spec_helper'

describe RailsRedshiftReplicator::Exporter::IdentityReplicator, type: :redshift_replicator, replicator: true do
  let(:user_replicatable) { RailsRedshiftReplicator::Replicatable.new(:identity_replicator, source_table: :users) }
  describe "#replication_field", :focus do
    it "returns the replicatable replication_field" do
      expect(RailsRedshiftReplicator::Exporter::IdentityReplicator.new(user_replicatable).replication_field).to eq 'id'
    end
  end
end