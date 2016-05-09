require 'spec_helper'

describe RailsRedshiftReplicator::Model::Extension do
  before { RailsRedshiftReplicator.debug_mode = true }

  describe '.export', focus: true do
    it 'forwards the command to its replicable class' do
      expect(RailsRedshiftReplicator.replicables['users']).to receive(:export)
      User.rrr_export
    end
  end
  describe '.import', focus: true do
    it 'forwards the command to its replicable class' do
      expect(RailsRedshiftReplicator.replicables['users']).to receive(:import)
      User.rrr_import
    end
  end
  describe '.replicate', focus: true do
    it 'forwards the command to its replicable class' do
      expect(RailsRedshiftReplicator.replicables['users']).to receive(:replicate)
      User.rrr_replicate
    end
  end
  describe '.vacuum', focus: true do
    it 'forwards the command to its replicable class' do
      expect(RailsRedshiftReplicator.replicables['users']).to receive(:vacuum)
      User.rrr_vacuum
    end
  end
  describe '.analyze', focus: true do
    it 'forwards the command to its replicable class' do
      expect(RailsRedshiftReplicator.replicables['users']).to receive(:analyze)
      User.rrr_analyze
    end
  end
end