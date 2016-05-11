require 'spec_helper'

describe RailsRedshiftReplicator::Importers::Base do
  let(:replication) { build :redshift_replication, target_table: "users", key: 'rrr/users/users_1.csv.0'}
  let(:importer) { RailsRedshiftReplicator::Importers::IdentityReplicator.new(replication) }

  describe '#evaluate_history_cap' do
    before { 10.times {create :redshift_replication, source_table: 'users'} }
    let!(:keep1) { create :redshift_replication, source_table: 'users' }
    let!(:keep2) { create :redshift_replication, source_table: 'users' }
    let!(:keep3) { create :redshift_replication, source_table: 'users' }
    before { RailsRedshiftReplicator.history_cap = 3 }
    it 'keeps only the records allowed by the history cap' do
      importer.evaluate_history_cap
      expect(RailsRedshiftReplicator::Replication.where(source_table: 'users')).to contain_exactly(keep1, keep2, keep3)
    end
  end
  describe "#copy" do
    let(:exporter) { user_exporter }
    before(:all) do
      recreate_users_table
      exporter = RailsRedshiftReplicator::Exporters::Base
      exporter.replication_bucket.files.create key: exporter.s3_file_key('users','valid_user.csv'), body: replication_file("valid_user.csv")
      exporter.replication_bucket.files.create key: exporter.s3_file_key('users','invalid_user.csv'), body: replication_file("invalid_user.csv")
    end
    context "with valid file" do
      before { importer.replication.key = RailsRedshiftReplicator::Exporters::Base.s3_file_key('users','valid_user.csv')}
      context "when flag as imported option is set to true" do
        let(:options) { {mark_as_imported: true} }
        it "flags as imported" do
          expect{ importer.copy(importer.replication.target_table, options) }.to change(importer.replication, :state).to("imported")
        end
      end
      context "when option to flag as imported is false" do
        let(:options) { {mark_as_imported: false} }
        it "doesn't flag as imported" do
          expect{importer.copy(importer.replication.target_table, options)}.not_to change(importer.replication, :state)
        end
      end
    end
    context "with invalid file" do
      let(:options) { {mark_as_imported: true} }
      before { importer.replication.key = RailsRedshiftReplicator::Exporters::Base.s3_file_key('users','invalid_user.csv')}
      it "finds error on redshift" do
        expect(importer).to receive(:get_redshift_error)
        importer.copy(importer.replication.target_table, options)
      end
      it "notifies error" do
        expect(importer).to receive(:notify_error)
        importer.copy(importer.replication.target_table, options)
      end
      it "doesn't drop target table on error by default" do
        expect(importer).not_to receive(:drop_table)
        importer.copy(importer.replication.target_table, options)
      end
      context 'when can drop temporary table on error' do
        let(:options) { {mark_as_imported: true, can_drop_target_on_error: true} }
        it 'drops table' do
          expect(importer).to receive(:drop_table)
          importer.copy(importer.replication.target_table, options)
        end
      end
    end
  end

  describe "#import_file" do
    it "returns s3 location for file" do
      expect(importer.import_file).to eq('s3://rrr-s3-test/rrr/users/users_1.csv.0')
    end
  end

  describe "#copy_statement" do
    before do
      allow(importer).to receive(:import_file).and_return('users')
      allow(RailsRedshiftReplicator).to receive(:aws_credentials).and_return({key: 1, secret: 2})
    end
    let(:statement) { "COPY users from 'users' REGION 'us-east-1' credentials 'aws_access_key_id=1;aws_secret_access_key=2' maxerror 0 acceptinvchars STATUPDATE true CSV"}
    it "returns sql to execute COPY" do
      expect(importer.copy_statement("users")).to eq(statement)
    end
  end

  describe "#drop_table" do
    let(:sql) { "drop table if exists table_name" }
    it "drops the temporary table created for the replication" do
      expect(RailsRedshiftReplicator.connection).to receive(:exec).with(sql).once
      importer.drop_table("table_name")
    end
  end

  describe "#temporary_table_name" do
    it "returns a random name for the temporary table" do
      Timecop.freeze do
        now = Time.now.to_i
        expect(importer.temporary_table_name).to eq("temp_users_#{now}")
      end
    end
  end

end
