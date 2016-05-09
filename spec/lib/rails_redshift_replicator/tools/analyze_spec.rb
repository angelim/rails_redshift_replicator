require 'spec_helper'
describe RailsRedshiftReplicator::Tools::Analyze do
  context "when uses :all for all tables" do
    it "executes" do
      expect(RailsRedshiftReplicator.connection).to receive(:exec).with("ANALYZE ;")
      RailsRedshiftReplicator.analyze(:all)
    end
  end
  context "with one table" do
    it "executes" do
      expect(RailsRedshiftReplicator.connection).to receive(:exec).with("ANALYZE users;")
      RailsRedshiftReplicator.analyze("users")
    end
  end
end