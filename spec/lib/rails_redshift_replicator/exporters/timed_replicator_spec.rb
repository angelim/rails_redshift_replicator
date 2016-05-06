require 'spec_helper'

describe RailsRedshiftReplicator::Exporter::TimedReplicator, type: :redshift_replicator, replicator: true do
  let(:user_replicatable) { RailsRedshiftReplicator::Replicatable.new(:timed_replicator, source_table: :posts) }
  describe "#replication_field", :focus do
    it "returns the replicatable replication_field" do
      expect(RailsRedshiftReplicator::Exporter::TimedReplicator.new(user_replicatable).replication_field).to eq 'updated_at'
    end
  end
end