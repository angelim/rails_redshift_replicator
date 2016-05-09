require 'spec_helper'

describe 'RailsRedshiftReplicator::Importers::TimedReplicator' do
  let(:exporter) { RailsRedshiftReplicator::Exporters::Base  }
  describe "import" do
    before(:all) { recreate_posts_table }
    let!(:replication) do
      create :redshift_replication,
             target_table: 'posts',
             key: exporter.s3_file_key('posts','valid_post.csv'),
             state: 'uploaded',
             replication_type: 'TimedReplicator',
             export_format: 'csv'
    end
    let(:post_importer) { RailsRedshiftReplicator::Importers::TimedReplicator.new(replication) }
    before do
      exporter.replication_bucket.files.create key: exporter.s3_file_key('posts','valid_post.csv'),
                                               body: replication_file("valid_post.csv")
    end
    it "performs import" do
      expect { post_importer.import; replication.reload }.to change(replication, :state).from("uploaded").to("imported")
    end
  end
end