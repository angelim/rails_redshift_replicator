require 'spec_helper'
require 'active_support/core_ext/time/calculations'

describe RailsRedshiftReplicator::Exporters::Base do
  let(:user_replicable)  { RailsRedshiftReplicator::Replicable.new(:identity_replicator, source_table: :users)}
  let(:post_replicable)  { RailsRedshiftReplicator::Replicable.new(:timed_replicator, source_table: :posts)}
  let(:full_replicable)  { RailsRedshiftReplicator::Replicable.new(:full_replicator, source_table: :tags_users)}
  let(:user_exporter)      { RailsRedshiftReplicator::Exporters::IdentityReplicator.new(user_replicable) }
  let(:post_exporter)      { RailsRedshiftReplicator::Exporters::TimedReplicator.new(post_replicable) }
  let(:habtm_exporter)     { RailsRedshiftReplicator::Exporters::FullReplicator.new(full_replicable) }
  before { RailsRedshiftReplicator.debug_mode = true }

  describe ".s3_file_key" do
    it "returns s3 handler for file" do
      expect(RailsRedshiftReplicator::Exporters::Base.s3_file_key("users","file.csv")).to eq "rrr/users/file.csv"
    end
  end

  # Requires previous bucket creation on s3
  describe ".replication_bucket" do
    it "returns a connection to the s3 replication bucket defined during setup" do
      expect(RailsRedshiftReplicator::Exporters::Base.replication_bucket).to be_a ::Fog::Storage::AWS::Directory
      expect(RailsRedshiftReplicator::Exporters::Base.replication_bucket.key).to eq "rrr-s3-test"
    end
  end

  describe ".s3_connection" do
    it "returns connection to s3" do
      expect(RailsRedshiftReplicator::Exporters::Base.s3_connection).to be_a ::Fog::Storage::AWS::Real
    end
  end

  describe 'Instance Methods' do
    describe "#table_indexes" do
      context "with User model" do
        it "returns user's table indexes" do
          expect(user_exporter.table_indexes).to contain_exactly("id", "login", "age")
        end
      end
    end

    describe '#connection_adapter' do
      context 'when Mysql2' do
        before { allow(user_exporter.ar_client).to receive(:adapter_name).and_return("Mysql2") }
        it 'returns RailsRedshiftReplicator::Adapters::Mysql2' do
          expect(user_exporter.connection_adapter).to be_an RailsRedshiftReplicator::Adapters::Mysql2
        end
      end
      context 'when PostgreSQL' do
        before { allow(user_exporter.ar_client).to receive(:adapter_name).and_return("PostgreSQL") }
        it 'returns RailsRedshiftReplicator::Adapters::Postgresql' do
          expect(user_exporter.connection_adapter).to be_an RailsRedshiftReplicator::Adapters::PostgreSQL
        end
      end
      context 'when SQLite' do
        before { allow(user_exporter.ar_client).to receive(:adapter_name).and_return("SQLite") }
        it 'returns RailsRedshiftReplicator::Adapters::Sqlite' do
          expect(user_exporter.connection_adapter).to be_an RailsRedshiftReplicator::Adapters::SQLite
        end
      end
    end

    describe "#has_index?" do
      before { allow(user_exporter).to receive(:table_indexes).and_return [:id, :updated_at] }
      context "when replication_field has an index" do
        before { allow(user_exporter).to receive(:replication_field).and_return(:updated_at) }
        it "returns true" do
          expect(user_exporter).to be_has_index
        end
      end
      context "when replication_field doesn't have an index" do
        before { allow(user_exporter).to receive(:replication_field).and_return(:login) }
        it "returns false" do
          expect(user_exporter).not_to be_has_index
        end
      end
    end


    describe "#replication_field" do
      it "returns the replicable replication_field" do
        expect(RailsRedshiftReplicator::Exporters::Base.new(user_replicable).replication_field).to eq 'id'
      end
    end

    describe "#check_target_table" do
      context "when the target table exists on redshift" do
        before { recreate_users_table }
        it "doesn't set an error" do
          user_exporter.check_target_table
          expect(user_exporter.errors).to be_nil
        end
      end
      context "when the target table doesn't exist on redshift" do
        before(:all) { drop_redshift_table(:users) }
        after(:all) { recreate_users_table }
        it "sets the exporter to an error state" do
          user_exporter.check_target_table
          expect(user_exporter.errors).to be_present
        end
      end
    end

    describe '#query_command' do
      before { allow(user_exporter).to receive(:fields_to_sync).and_return(%w(id user_id publication_id)) }
      before { allow(user_exporter).to receive(:last_record).and_return(10) }
      
      let(:sql) { "SELECT id,user_id,publication_id FROM users WHERE 1=1 AND id > '5' AND id <= '10' OR id IS NULL" }
      context 'when adapter is Mysql2' do
        before { allow(user_exporter.ar_client).to receive(:adapter_name).and_return('Mysql2') }
        it "executes query with streaming option" do
          expect(user_exporter.ar_client.instance_variable_get("@connection")).to receive(:query).with(sql, stream: true)
          user_exporter.records(5)
        end
      end
      context "when adapter is SQLite" do
        before { allow(user_exporter.ar_client).to receive(:adapter_name).and_return('SQLite') }
        it "executes query without other arguments" do
          expect(user_exporter.ar_client).to receive(:exec_query).with(sql)
          user_exporter.records(5)
        end
      end
      context "when adapter is PostgreSQL" do
        before { allow(user_exporter.ar_client).to receive(:adapter_name).and_return('PostgreSQL') }
        xit "executes query using single row mode" do
        end
      end
    end

    describe '#records' do
      
    end

    describe "#build_query_sql" do
      before { allow(user_exporter).to receive(:fields_to_sync).and_return(%w(id user_id publication_id)) }
      context "when there's a last record" do
        before { allow(user_exporter).to receive(:last_record).and_return(10) }
        context "when an initial record is given" do
          let(:sql) { "SELECT id,user_id,publication_id FROM users WHERE 1=1 AND id > '5' AND id <= '10' OR id IS NULL" }
          it "builds correct sql" do
            expect(user_exporter.build_query_sql(5)).to eq sql
          end
        end
        context "when an initial record isn't given" do
          let(:sql) { "SELECT id,user_id,publication_id FROM users WHERE 1=1 AND id <= '10' OR id IS NULL" }
          it "builds correct sql" do
            expect(user_exporter.build_query_sql).to eq sql
          end
        end
      end
      context "when a last_record can't be found" do
        before { allow(user_exporter).to receive(:last_record).and_return(nil) }
        context "when an initial record is given" do
          let(:sql) { "SELECT id,user_id,publication_id FROM users WHERE 1=1 AND id > '5'" }
          it "builds correct sql" do
            expect(user_exporter.build_query_sql(5)).to eq sql
          end
        end
        context "when an initial record isn't given" do
          let(:sql) { "SELECT id,user_id,publication_id FROM users WHERE 1=1" }
          it "builds correct sql" do
            expect(user_exporter.build_query_sql).to eq sql
          end
        end
      end
    end

    describe "#from_record" do
      context "when there's a previous replication record" do
        let!(:replication) { create :redshift_replication, target_table: 'users', last_record: 1 }
        let!(:replication2) { create :redshift_replication, target_table: 'users', last_record: 5 }
        it "returns the id or timestamp of the last_record on the most recent complete replication" do
          expect(user_exporter.from_record).to eq '5'
        end
      end
      context "when there isn't a previous replication record" do
        it "returns nil" do
          expect(user_exporter.from_record).to be_nil
        end
      end
    end

    describe "#initialize_replication" do
      before do
        allow(user_exporter).to receive(:last_record).and_return(3)
        allow(user_exporter).to receive(:from_record).and_return(1)
        user_exporter.initialize_replication("replication_file", "csv", 2)
      end
      let(:replication) { user_exporter.replication }

      it "creates a replication record" do
        expect(replication.export_format).to    eq "csv"
        expect(replication.slices).to           eq 2
        expect(replication.record_count).to     be_nil
        expect(replication.key).to              eq "rrr/users/replication_file"
        expect(replication.last_record).to      eq '3'
        expect(replication.first_record).to     eq 1
        expect(replication.state).to            eq "exporting"
        expect(replication.replication_type).to eq "identity_replicator"
        expect(replication.target_table).to     eq "users"
        expect(replication.source_table).to     eq "users"
      end
    end

    describe "#row_count_threshold" do
      before { user_exporter.replication = build :redshift_replication, slices: 3 }
      it "returns number of lines to split export files" do
        expect(user_exporter.row_count_threshold(200)).to eq 67
      end
    end

    describe "#write_csv" do
    end

    describe "#export" do
      before { allow(user_exporter).to receive(:fields_to_sync).and_return(%w(id login age)) }
      context "when there are incomplete replications" do
        before { create :redshift_replication, source_table: "users", state: "uploaded" }
        it "doesn't create replication record" do
          user_exporter.export
          expect(user_exporter.replication).to be_nil
        end
        it "returns nil" do
          expect(user_exporter.export).to be_nil
        end
      end
      context "when there are no incomplete replications" do
        before { create :redshift_replication, source_table: "users", state: "imported" }
        context "and there are records to export" do
          before { create :user }
          context 'and exporter is in error state' do
            before { user_exporter.errors = 'some error' }
            it 'returns false' do
              expect(user_exporter.export).to be_nil
            end
          end
          context 'and exporter is valid' do
            it "creates export files" do
              file_paths = user_exporter.export
              expect(file_paths).to be_an_instance_of Array
              expect(file_paths).to be_present
            end
            it "flags replication as exported" do
              user_exporter.export
              expect(user_exporter.replication.state).to eq "exported"
            end
          end
        end
        context "and there aren't any records to export" do
          it "doesn't create replication record" do
            user_exporter.export
            expect(user_exporter.replication).to be_nil
          end
          it "retorns nil" do
            expect(user_exporter.export).to be_nil
          end
        end
      end
    end

    describe "#split_file" do
      before do
        allow(user_exporter).to receive(:local_file).and_return "/tmp/test"
      end

      it "splits file in 4 parts" do
        f = File.open("/tmp/test", "w")
        100.times{ f.puts "nothing here" }
        f.close
        user_exporter.split_file("test", 4)
        %w(aa ab ac ad).each do |suffix|
          File.exists?("/tmp/test.#{suffix}").should be true
        end
      end
    end

    describe "#file_key_in_format" do
      context "with csv format" do
        let(:file) { user_exporter.file_key_in_format("file.csv", "csv") }
        it "returns file handler on s3" do
          expect(file).to eq "rrr/users/file.csv"
        end
      end
      context "with gzip format" do
        let(:file) { user_exporter.file_key_in_format("file.csv", "gzip") }
        it "returns file handler on s3" do
          expect(file).to eq "rrr/users/file.gz"
        end
      end

    end

    describe "#gzipped" do
      it "returns gz file extension" do
        expect(user_exporter.gzipped("file.csv")).to eq "file.gz"
      end
    end

    describe "#last_replication" do
      let!(:replication1) { create :redshift_replication, source_table: "users" }
      let!(:replication2) { create :redshift_replication, source_table: "users" }
      let!(:replication3) { create :redshift_replication, source_table: "posts" }
      it "retuns the last replication record for a given table" do
        expect(user_exporter.last_replication).to eq replication2
      end
    end

    describe "#pending_imports?" do
      context "when the last replication is complete" do
        before { create :redshift_replication, source_table: "users", state: "imported"}
        it "returns false" do
          expect(user_exporter).not_to be_pending_imports
        end
      end

      context "when there are no previous replications for the table" do
        it "returns false" do
          expect(user_exporter).not_to be_pending_imports
        end
      end
      context "when the last replication is not complete" do
        before { create :redshift_replication, source_table: "users", state: "uploaded"}
        it "returns true" do
          expect(user_exporter).to be_pending_imports
        end
      end
    end

    describe "#export_and_upload" do
      context "when the target table doesn't exist on redshift" do
        before(:all) { drop_redshift_table(:users) }
        after(:all)  { recreate_users_table }
        it "doesn't call #export" do
          expect(user_exporter.export_and_upload).not_to receive(:export)
        end
      end
      context "when the target table exists on redshift" do
        before(:all) { recreate_users_table }
        it "calls #export" do
          expect(user_exporter).to receive(:export)
          user_exporter.export_and_upload
        end
        context "when there are records to export" do
          before { allow(user_exporter).to receive(:export).and_return("file") }
          it "calls #upload" do
            expect(user_exporter).to receive(:upload)
            user_exporter.export_and_upload
          end
        end
        context "when there are no records to export" do
          it "doesn't call #upload" do
            expect(user_exporter.export_and_upload).not_to receive(:upload)
          end
        end

      end
    end

    describe "#upload" do
      context "with csv format" do
        let(:files) { %w(file1 file2) }
        before { user_exporter.replication = create(:redshift_replication, source_table: "users", export_format: "csv") }
        before { user_exporter.file_names = files }
        context 'when exporter is in an error state' do
          before { user_exporter.errors = "some error"}
          it 'returns nil' do
            expect(user_exporter.upload).to be_nil 
          end
        end
        context 'when exporter is valid' do
          it "calls csv uploader" do
            expect(user_exporter).to receive(:upload_csv).with(files)
            user_exporter.upload
          end
          it "changes replication state to uploaded" do
            expect(user_exporter).to receive(:upload_csv).with(files)
            user_exporter.upload
            expect(user_exporter.replication.state).to eq "uploaded"
          end
        end
      end
    end

    describe "#upload_csv" do
      let(:file_names) { ["file", "file.aa", "file.ab"] }
      let!(:files) { file_names.map{ |f| File.open("/tmp/#{f}", "w") } }
      before { user_exporter.replication = build(:redshift_replication, key: "file") }
      it "uploads the splitted files" do
        user_exporter.upload_csv(files.map(&:path))
        expect(RailsRedshiftReplicator::Exporters::Base.replication_bucket.files.get("rrr/users/file")).not_to be_present
        expect(RailsRedshiftReplicator::Exporters::Base.replication_bucket.files.get("rrr/users/file.aa")).to be_present
        expect(RailsRedshiftReplicator::Exporters::Base.replication_bucket.files.get("rrr/users/file.ab")).to be_present
      end

      it "deletes local files afterwards" do
        user_exporter.upload_csv(files.map(&:path))
        file_names.each do |file|
          expect(File.exists?("/tmp/#{file}")).to be false
        end
      end
    end

    describe "#last_record" do
      let!(:user1) { create :user }
      let!(:post1) { create :post }
      context "when the last replication record uses time based replication" do
        it "returns a date" do
          Timecop.freeze Date.tomorrow do
            post2 = create :post
            formatted = Time.parse(post_exporter.last_record).to_s(:db)
            expect(formatted).to eq post2.updated_at.to_s(:db)
          end
        end
      end
      context "when the last replication record uses an identity column" do
        let!(:post)  { create :post }
        it "returns the id" do
          user1 = create :user
          user2 = create :user
          expect(user_exporter.last_record.to_s).to eq user2.id.to_s
        end
      end
    end

    describe "fields_to_sync" do
      before { recreate_users_table }
      it "returns list of columns on the target table on redshift" do
        expect(user_exporter.fields_to_sync).to contain_exactly("age", "confirmed", "created_at", "id", "login", "updated_at")
      end
    end
  end
end
