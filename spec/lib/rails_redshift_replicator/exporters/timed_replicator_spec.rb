require 'spec_helper'

describe RailsRedshiftReplicator::Exporters::TimedReplicator do
  let(:file_manager)  { RailsRedshiftReplicator::FileManager.new}
  let(:replicable)    { RailsRedshiftReplicator::Replicable.new(:full_replicator, source_table: :tags_users) }
  let(:s3_bucket)     { Aws::S3::Bucket.new(name: RailsRedshiftReplicator.s3_bucket_params[:bucket], client: file_manager.s3_client) }
  let(:replicable)    { RailsRedshiftReplicator::Replicable.new(:timed_replicator, source_table: :posts) }
  describe "#replication_field", :focus do
    it "returns the replicable replication_field" do
      expect(RailsRedshiftReplicator::Exporters::TimedReplicator.new(replicable).replication_field).to eq 'updated_at'
    end
  end
  describe 'Integration Test' do
    before(:all) { recreate_posts_table }
    it "exports timed replicator type replication" do
      model = :post
      # first export
      instance = create model
      RailsRedshiftReplicator::Exporters::TimedReplicator.new(replicable).export_and_upload
      replication1 = RailsRedshiftReplicator::Replication.from_table(model.to_s.pluralize).last
      expect(replication1.state).to eq "uploaded"
      expect(replication1.record_count).to eq 1
      file_body = s3_bucket.object("#{replication1.key}.aa").get.body.read
      # file_body = RailsRedshiftReplicator::Exporters::Base.replication_bucket.files.get("#{replication1.key}.aa").body
      expect(file_body).to match(/#{instance.id},#{instance.user_id},#{instance.content}/)
      # export without new records
      RailsRedshiftReplicator::Exporters::TimedReplicator.new(replicable).export_and_upload
      expect(RailsRedshiftReplicator::Replication.from_table(model.to_s.pluralize).last).to eq replication1
      replication1.imported!
      # export after creating new records
      3.times {create model}
      RailsRedshiftReplicator::Exporters::TimedReplicator.new(replicable).export_and_upload
      replication2 = RailsRedshiftReplicator::Replication.from_table(model.to_s.pluralize).last
      expect(replication2.record_count).to eq 3
      replication2.imported!
      # export after updating first record
      instance.touch
      RailsRedshiftReplicator::Exporters::TimedReplicator.new(replicable).export_and_upload
      replication3 = RailsRedshiftReplicator::Replication.from_table(model.to_s.pluralize).last
      expect(replication3.record_count).to eq 1
    end
  end
end