require 'spec_helper'

describe RailsRedshiftReplicator::Model::Extension, type: :redshift_replicator, replicator: true do
  describe "Integration tests" do
    describe 'IdentityReplicator' do
      before(:all) { recreate_users_table }
      it "exports users" do
        model = :user
        instance = create model
        User.rails_redshift_replicator_export
        replication1 = RailsRedshiftReplicator::Replication.from_table(model.to_s.pluralize).last
        expect(replication1.state).to eq "uploaded"
        expect(replication1.record_count).to eq 1
        file_body = RailsRedshiftReplicator::Exporters::Base.replication_bucket.files.get("#{replication1.key}.aa").body
        expect(file_body).to match(/#{instance.id},#{instance.login},#{instance.age}/)
      end
    end
    describe 'TimedReplicator' do
      before(:all) { recreate_posts_table }
      it "exports users" do
        model = :post
        instance = create model
        Post.rails_redshift_replicator_export
        replication1 = RailsRedshiftReplicator::Replication.from_table(model.to_s.pluralize).last
        expect(replication1.state).to eq "uploaded"
        expect(replication1.record_count).to eq 1
        file_body = RailsRedshiftReplicator::Exporters::Base.replication_bucket.files.get("#{replication1.key}.aa").body
        expect(file_body).to match(/#{instance.id},#{instance.user_id},#{instance.content}/)
      end
    end
    describe 'FullReplicator' do
      before(:all) { recreate_tags_users_table }
      before { RailsRedshiftReplicator.add_replicatable({ "tags_users" => RailsRedshiftReplicator::Replicatable.new(:full_replicator, source_table: :tags_users) }) }
      it "exports full replicator type replication" do
        model = :tags_users
        # first export
        tag = create :tag
        user = create :user
        tag.users << user
        RailsRedshiftReplicator.replicatables[:tags_users].export
        replication1 = RailsRedshiftReplicator::Replication.from_table("tags_users").last
        expect(replication1.state).to eq "uploaded"
        expect(replication1.record_count).to eq 1
        file_body = RailsRedshiftReplicator::Exporters::Base.replication_bucket.files.get("#{replication1.key}.aa").body
        expect(file_body).to match(/#{user.id},#{tag.id}/)
      end
    end
  end
end