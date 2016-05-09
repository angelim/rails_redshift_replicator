require 'spec_helper'
def redshift_counts(table)
  RailsRedshiftReplicator.connection.exec("SELECT COUNT(1) FROM #{table}").first["count"].to_i
end
describe 'Integration Tests' do
  before(:all) { RailsRedshiftReplicator.debug_mode = true}
  let(:user_replicable)  { RailsRedshiftReplicator.replicables[:users] }
  let(:post_replicable)  { RailsRedshiftReplicator.replicables[:posts] }
  let(:habtm_replicable) { RailsRedshiftReplicator::Replicable.new(:full_replicator, source_table: :tags_users)}
  
  before do
    RailsRedshiftReplicator.add_replicable({ "tags_users" => habtm_replicable})
  end

  describe '.replicate' do
    context 'replicating users' do
      before(:all) { recreate_users_table }
      before do
        5.times { create :user }
      end
      it 'replicates 5 users' do
        RailsRedshiftReplicator.replicate :users
        expect(redshift_counts('users')).to eq 5
      end
    end
    context 'replicating users and posts' do
      before(:all) do
        recreate_users_table
        recreate_posts_table
      end
      it 'replicates 15 users and 10 posts' do
        5.times { create :user }
        10.times { create :post } # creates one user for each post

        RailsRedshiftReplicator.replicate :users, :posts
        expect(redshift_counts('users')).to eq 15
        expect(redshift_counts('posts')).to eq 10
      end
      context 'replicating users and posts' do
        before(:all) do
          recreate_users_table
          recreate_posts_table
          recreate_tags_users_table
          recreate_tags_table(:custom_tags)
        end
        it 'replicates 15 users, 10 posts, 25 tags_users and 25 tags', focus: true do
          5.times { create :user_with_tags }
          10.times { create :post } # creates one user for each post

          RailsRedshiftReplicator.replicate :all
          expect(redshift_counts('users')).to eq 15
          expect(redshift_counts('posts')).to eq 10
          expect(redshift_counts('tags_users')).to eq 25
          expect(redshift_counts('custom_tags')).to eq 25
        end
      end
    end

  end

  describe ".export" do
    before {User;Post} # Load Models
    it 'calls export for replicables' do
      expect(user_replicable).to receive(:export)
      expect(post_replicable).to receive(:export)
      RailsRedshiftReplicator.export(:users, :posts)
    end
    it 'does not call export for non replicables' do
      expect_any_instance_of(RailsRedshiftReplicator::Replicable).not_to receive(:export)
      RailsRedshiftReplicator.export(:non_replicable)
    end
  end

  describe ".import" do
    before {User;Post} # Load Models
    it 'calls import for replicables' do
      expect(user_replicable).to receive(:import)
      expect(post_replicable).to receive(:import)
      RailsRedshiftReplicator.import(:users, :posts)
    end
    it 'does not call import for non replicables' do
      expect_any_instance_of(RailsRedshiftReplicator::Replicable).not_to receive(:import)
      RailsRedshiftReplicator.import(:non_replicable)
    end
  end
end