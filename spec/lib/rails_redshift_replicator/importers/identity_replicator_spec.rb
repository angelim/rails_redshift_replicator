require 'spec_helper'

describe 'RailsRedshiftReplicator::Importers::IdentityReplicator' do
  let(:exporter) { RailsRedshiftReplicator::Exporters::Base  }
  describe "import" do
    before(:all) { recreate_users_table }
    let!(:replication) do
      create :redshift_replication,
             target_table: 'users',
             key: exporter.s3_file_key('users','valid_user.csv'),
             state: 'uploaded',
             replication_type: 'IdentityReplicator',
             export_format: 'csv'
    end
    let(:user_importer)    { RailsRedshiftReplicator::Importers::IdentityReplicator.new(replication) }
    before do
      exporter.replication_bucket.files.create key: exporter.s3_file_key('users','valid_user.csv'),
                                               body: replication_file("valid_user.csv")
    end
    it "performs import" do
      expect { user_importer.import; replication.reload }.to change(replication, :state).from("uploaded").to("imported")
    end
  end
end