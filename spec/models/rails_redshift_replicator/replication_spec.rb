# encoding: utf-8
require 'spec_helper'

describe RailsRedshiftReplicator::Replication, type: :model do

  it { is_expected.to validate_presence_of(:replication_type) }
  it { is_expected.to validate_presence_of(:key) }
  it { is_expected.to validate_presence_of(:source_table) }
  it { is_expected.to validate_presence_of(:target_table) }
  it { is_expected.to validate_inclusion_of(:state).in_array RailsRedshiftReplicator::Replication::STATES }
  it { is_expected.to validate_inclusion_of(:export_format).in_array RailsRedshiftReplicator::Replication::FORMATS }

  let(:redshift_replication) { build :redshift_replication }
  describe "Format helper methods" do
    RailsRedshiftReplicator::Replication::FORMATS.each do |format|
      it "responds to ##{format}?" do
        expect(redshift_replication).to respond_to "#{format}?"
      end
    end
  end
  describe "State helper methods" do
    RailsRedshiftReplicator::Replication::STATES.each do |state|
      it "responds to ##{state}?" do
        expect(redshift_replication).to respond_to "#{state}?"
      end
      it "responds to ##{state}!" do
        expect(redshift_replication).to respond_to "#{state}!"
      end
      it "responds to scope .#{state}" do
        expect(RailsRedshiftReplicator::Replication).to respond_to state
      end
    end
  end
  describe "format #csv?" do
    let(:redshift_replication) { build :redshift_replication, export_format: "csv" }
    context "when format matches" do
      it "returns true" do
        expect(redshift_replication.csv?).to be true
      end
    end
    context "when format does not match" do
      it "returns false" do
        expect(redshift_replication.gzip?).to be false
      end
    end
  end
  describe "state #uploading" do
    let(:redshift_replication) { build :redshift_replication, state: "uploading" }
    context "when state matches" do
      it "returns true" do
        expect(redshift_replication.uploading?).to be true
      end
    end
    context "when state does not match" do
      it "returns false" do
        expect(redshift_replication.exporting?).to be false
      end
    end
  end
  describe "state #exported!" do
    let(:redshift_replication) { create(:redshift_replication, state: 'enqueued') }
    it "changes stage to :exporting" do
      redshift_replication.exported!
      expect(redshift_replication.state).to eq 'exported'
    end
    it "updates aditional fields" do
      redshift_replication.exported!(export_duration: 10)
      expect(redshift_replication.export_duration).to eq 10
    end
    it "returns update time" do
      allow(Time).to receive(:now).and_return(Time.new(2016,1,1))
      now = Time.now
      expect(redshift_replication.exported!).to eq(now)
    end
  end
  describe "scope .exporting" do
    let!(:rep1) { create(:redshift_replication, state: 'enqueued') }
    let!(:rep2) { create(:redshift_replication, state: 'exporting') }
    it "returns replications in :exporting state" do
      expect(RailsRedshiftReplicator::Replication.exporting.to_a).to eq [rep2]
    end
  end
end
