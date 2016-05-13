require 'spec_helper'

describe RailsRedshiftReplicator::FileManager do
  let(:user_replicable)  { RailsRedshiftReplicator::Replicable.new(:identity_replicator, source_table: :users)}
  let(:replication)      { build :redshift_replication, slices: 4 }
  let(:user_exporter)    { RailsRedshiftReplicator::Exporters::IdentityReplicator.new(user_replicable, replication) }
  let(:file_manager)     { RailsRedshiftReplicator::FileManager.new(user_exporter)}

  describe ".s3_file_key" do
    it "returns s3 handler for file" do
      expect(RailsRedshiftReplicator::FileManager.s3_file_key("users","file.csv")).to eq "rrr/users/file.csv"
    end
  end

  describe "#s3_client" do
    it "returns connection to s3" do
      expect(file_manager.s3_client).to be_a Aws::S3::Client
    end
  end


  describe "#row_count_threshold" do
    before { user_exporter.replication = build :redshift_replication, slices: 3 }
    it "returns number of lines to split export files" do
      expect(file_manager.row_count_threshold(200)).to eq 67
    end
  end

  describe "#write_csv" do
  end

  describe "#split_file" do
    before do
      allow(file_manager).to receive(:local_file).and_return "/tmp/test"
    end

    it "splits file in 4 parts" do
      f = File.open("/tmp/test", "w")
      100.times{ f.puts "nothing here" }
      f.close
      file_manager.split_file("test", 4)
      %w(aa ab ac ad).each do |suffix|
        File.exists?("/tmp/test.#{suffix}").should be true
      end
    end
  end

  describe "#file_key_in_format" do
    context "with csv format" do
      let(:file) { file_manager.file_key_in_format("file.csv", "csv") }
      it "returns file handler on s3" do
        expect(file).to eq "rrr/users/file.csv"
      end
    end
    context "with gzip format" do
      let(:file) { file_manager.file_key_in_format("file.csv", "gzip") }
      it "returns file handler on s3" do
        expect(file).to eq "rrr/users/file.gz"
      end
    end

  end

  describe "#gzipped" do
    it "returns gz file extension" do
      expect(file_manager.gzipped("file.csv")).to eq "file.gz"
    end
  end

  describe "#upload_csv", focus: true do
    let(:bucket) { RailsRedshiftReplicator.s3_bucket_params[:bucket]}
    let(:s3_bucket) { Aws::S3::Bucket.new(name: bucket, client: file_manager.s3_client) }
    let(:file_names) { ["file", "file.aa", "file.ab"] }
    let!(:files) { file_names.map{ |f| File.open("/tmp/#{f}", "w") } }
    before { user_exporter.replication = build(:redshift_replication, key: "file") }
    it "uploads the splitted files" do
      file_manager.upload_csv(files.map(&:path))
      expect(s3_bucket.object('rrr/users/file').exists?).to be false
      expect(s3_bucket.object('rrr/users/file.aa').exists?).to be true
      expect(s3_bucket.object('rrr/users/file.ab').exists?).to be true
    end

    it "deletes local files afterwards" do
      file_manager.upload_csv(files.map(&:path))
      file_names.each do |file|
        expect(File.exists?("/tmp/#{file}")).to be false
      end
    end
  end
end