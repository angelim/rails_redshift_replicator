require 'spec_helper'

describe RailsRedshiftReplicator::Exporters::IdentityReplicator do
  let(:replicable) { RailsRedshiftReplicator::Replicable.new(:identity_replicator, source_table: :users) }
  describe "#replication_field", :focus do
    it "returns the replicable replication_field" do
      expect(RailsRedshiftReplicator::Exporters::IdentityReplicator.new(replicable).replication_field).to eq 'id'
    end
  end
  describe 'Integration Test' do
    before(:all) { recreate_users_table }
    it "exports identity replicator type replication" do
      model = :user
      # first export
      instance = create model
      RailsRedshiftReplicator::Exporters::IdentityReplicator.new(replicable).export_and_upload
      replication1 = RailsRedshiftReplicator::Replication.from_table(model.to_s.pluralize).last
      expect(replication1.state).to eq "uploaded"
      expect(replication1.record_count).to eq 1
      file_body = RailsRedshiftReplicator::Exporters::Base.replication_bucket.files.get("#{replication1.key}.aa").body
      expect(file_body).to match(/#{instance.id},#{instance.login},#{instance.age}/)
      # export without new records
      RailsRedshiftReplicator::Exporters::IdentityReplicator.new(replicable).export_and_upload
      expect(RailsRedshiftReplicator::Replication.from_table(model.to_s.pluralize).last).to eq replication1
      replication1.imported!
      # export after creating new records
      3.times {create model}
      RailsRedshiftReplicator::Exporters::IdentityReplicator.new(replicable).export_and_upload
      replication2 = RailsRedshiftReplicator::Replication.from_table(model.to_s.pluralize).last
      expect(replication2.record_count).to eq 3
      replication2.imported!
      # export after updating first record
      instance.touch
      RailsRedshiftReplicator::Exporters::IdentityReplicator.new(replicable).export_and_upload
      expect(RailsRedshiftReplicator::Replication.from_table(model.to_s.pluralize).last).to eq replication2
    end
  end
end