require 'spec_helper'

describe 'RailsRedshiftReplicator::Importers::FullReplicator' do
  let(:exporter) { RailsRedshiftReplicator::Exporters::Base  }
  let(:file_manager) { RailsRedshiftReplicator::FileManager.new }
  describe "import" do
    before(:all) { recreate_tags_users_table }
    let!(:replication) do
      create :redshift_replication,
             target_table: 'tags_users',
             key: RailsRedshiftReplicator::FileManager.s3_file_key('tags_users','valid_tags_users.csv'),
             state: 'uploaded',
             replication_type: 'FullReplicator',
             export_format: 'csv'
    end
    let(:full_importer) { RailsRedshiftReplicator::Importers::FullReplicator.new(replication) }
    before do
      file_manager.s3_client.put_object key: RailsRedshiftReplicator::FileManager.s3_file_key('tags_users','valid_tags_users.csv'),
        body: replication_file("valid_tags_users.csv"),
        bucket: RailsRedshiftReplicator.s3_bucket_params[:bucket]
    end
    it "performs import" do
      expect { full_importer.import; replication.reload }.to change(replication, :state).from("uploaded").to("imported")
    end
  end
end

