require 'spec_helper'

describe RailsRedshiftReplicator::Exporters::FullReplicator do
  let(:file_manager)     { RailsRedshiftReplicator::FileManager.new}
  let(:replicable) { RailsRedshiftReplicator::Replicable.new(:full_replicator, source_table: :tags_users) }
  let(:s3_bucket) { Aws::S3::Bucket.new(name: RailsRedshiftReplicator.s3_bucket_params[:bucket], client: file_manager.s3_client) }
  describe "#replication_field", :focus do
    it "returns the replicable replication_field" do
      expect(RailsRedshiftReplicator::Exporters::FullReplicator.new(replicable).replication_field).to be_nil
    end
  end
  describe 'Integration Test' do
    before(:all) { recreate_tags_users_table }
    it "exports full replicator type replication" do
      model = :tags_users
      # first export
      tag = create :tag
      user = create :user
      tag.users << user
      RailsRedshiftReplicator::Exporters::FullReplicator.new(replicable).export_and_upload
      replication1 = RailsRedshiftReplicator::Replication.from_table("tags_users").last
      expect(replication1.state).to eq "uploaded"
      expect(replication1.record_count).to eq 1
      file_body = s3_bucket.object("#{replication1.key}.aa").get.body.read
      expect(file_body).to match(/#{user.id},#{tag.id}/)
      replication1.imported!
      # export without new records
      RailsRedshiftReplicator::Exporters::FullReplicator.new(replicable).export_and_upload
      replication2 = RailsRedshiftReplicator::Replication.from_table("tags_users").last
      expect(replication2.record_count).to eq 1
    end
  end
end