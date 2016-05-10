require 'spec_helper'
describe RailsRedshiftReplicator::Replicable do
  
  describe 'initialization' do
    context 'when all options are given' do
      let(:replicable) { RailsRedshiftReplicator::Replicable.new(:identity_replicator, source_table: 'users', target_table: 'custom_users', replication_field: 'created_at') }  
      it 'has the right properties' do
        expect(replicable.source_table).to eq 'users'
        expect(replicable.target_table).to eq 'custom_users'
        expect(replicable.replication_field).to eq 'created_at'
        expect(replicable.replication_type).to eq :identity_replicator
      end
    end
    context 'with default options(idendity)' do
      let(:replicable) { RailsRedshiftReplicator::Replicable.new(:identity_replicator, source_table: 'users') }  
      it 'has the right properties' do
        expect(replicable.source_table).to eq 'users'
        expect(replicable.target_table).to eq 'users'
        expect(replicable.replication_field).to eq 'id'
        expect(replicable.replication_type).to eq :identity_replicator
      end
    end
    context 'with default options(timed)' do
      let(:replicable) { RailsRedshiftReplicator::Replicable.new(:timed_replicator, source_table: 'users') }  
      it 'has the right properties' do
        expect(replicable.source_table).to eq 'users'
        expect(replicable.target_table).to eq 'users'
        expect(replicable.replication_field).to eq 'updated_at'
        expect(replicable.replication_type).to eq :timed_replicator
      end
    end
    context 'with default options(full)' do
      let(:replicable) { RailsRedshiftReplicator::Replicable.new(:full_replicator, source_table: 'users') }  
      it 'has the right properties' do
        expect(replicable.source_table).to eq 'users'
        expect(replicable.target_table).to eq 'users'
        expect(replicable.replication_field).to be_nil
        expect(replicable.replication_type).to eq :full_replicator
      end
    end
  end

  describe '#exporter_class' do
    context 'when exists' do
      let(:replicable) { RailsRedshiftReplicator::Replicable.new(:identity_replicator, source_table: 'users') }  
      it 'returns exporter class' do
        expect(replicable.exporter_class).to eq RailsRedshiftReplicator::Exporters::IdentityReplicator
      end
    end
    context "when doesn't exist" do
      let(:replicable) { RailsRedshiftReplicator::Replicable.new(:none, source_table: 'users') }  
      it 'raises error' do
        expect{replicable.exporter_class}.to raise_error(StandardError)
      end
    end
  end
  describe '#importer_class' do
    context 'when exists' do
      let(:replicable) { RailsRedshiftReplicator::Replicable.new(:identity_replicator, source_table: 'users') }  
      it 'returns exporter class' do
        expect(replicable.importer_class).to eq RailsRedshiftReplicator::Importers::IdentityReplicator
      end
    end
    context "when doesn't exist" do
      let(:replicable) { RailsRedshiftReplicator::Replicable.new(:none, source_table: 'users') }  
      it 'raises error' do
        expect{replicable.importer_class}.to raise_error(StandardError)
      end
    end
  end

  describe '#export' do
    let(:replicable) { RailsRedshiftReplicator::Replicable.new(:identity_replicator, source_table: 'users') }  
    context 'without previous replications' do
      it 'calls the correspondent exporter class' do
        allow(RailsRedshiftReplicator::Exporters::IdentityReplicator).to receive_message_chain(:new, :export_and_upload)
        expect(RailsRedshiftReplicator::Exporters::IdentityReplicator).to receive(:new).with(replicable, nil)
        replicable.export
      end
    end
    context 'with previous imported replication' do
      let!(:previous_replication) { create :redshift_replication, source_table: 'users', replication_type: 'identity_replicator', state: 'imported'}
      it 'calls the correspondent exporter class' do
        allow(RailsRedshiftReplicator::Exporters::IdentityReplicator).to receive_message_chain(:new, :export_and_upload)
        expect(RailsRedshiftReplicator::Exporters::IdentityReplicator).to receive(:new).with(replicable, nil)
        replicable.export
      end
    end
    context 'with previous uploading replication' do
      let!(:previous_replication) { create :redshift_replication, source_table: 'users', replication_type: 'identity_replicator', state: 'uploading' }
      context 'and max_retries is defined' do
        before { RailsRedshiftReplicator.max_retries = 0 }
        context 'and max_retries was reached' do
          it 'calls the correspondent exporter class without the previous replication' do
            allow(RailsRedshiftReplicator::Exporters::IdentityReplicator).to receive_message_chain(:new, :export_and_upload)
            expect(RailsRedshiftReplicator::Exporters::IdentityReplicator).to receive(:new).with(replicable, nil)
            replicable.export
          end
          it 'cancels the previous replication' do
            allow(RailsRedshiftReplicator::Exporters::IdentityReplicator).to receive_message_chain(:new, :export_and_upload)
            replicable.export
            expect(previous_replication.reload).to be_canceled
          end
        end
        context "and max_retries wasn't reached" do
          before { RailsRedshiftReplicator.max_retries = 1 }
          it 'calls the correspondent exporter class with previous replication' do
            allow(RailsRedshiftReplicator::Exporters::IdentityReplicator).to receive_message_chain(:new, :export_and_upload)
            expect(RailsRedshiftReplicator::Exporters::IdentityReplicator).to receive(:new).with(replicable, previous_replication)
            replicable.export
          end
        end
      end
      context 'and max_retries is nil' do
        before { RailsRedshiftReplicator.max_retries = nil }
        it 'calls the correspondent exporter class with previous replication' do
          allow(RailsRedshiftReplicator::Exporters::IdentityReplicator).to receive_message_chain(:new, :export_and_upload)
          expect(RailsRedshiftReplicator::Exporters::IdentityReplicator).to receive(:new).with(replicable, previous_replication)
          replicable.export
        end
      end

    end
    context 'with previous importing replication' do
      before { RailsRedshiftReplicator.max_retries = nil }
      let!(:previous_replication) { create :redshift_replication, source_table: 'users', replication_type: 'identity_replicator', state: 'importing' }
      it 'calls the correspondent importer class' do
        allow(RailsRedshiftReplicator::Importers::IdentityReplicator).to receive_message_chain(:new, :import)
        expect(RailsRedshiftReplicator::Importers::IdentityReplicator).to receive(:new).with(previous_replication)
        replicable.export
      end
    end
  end

  describe 'import' do
    let(:replicable)    { RailsRedshiftReplicator::Replicable.new(:identity_replicator, source_table: 'users') }  
    let!(:replication1) { create :redshift_replication, source_table: 'users', state: 'imported' }
    let!(:replication2) { create :redshift_replication, source_table: 'posts' }
    context 'without last replication' do
      it 'does not call import' do
        allow(RailsRedshiftReplicator::Importers::IdentityReplicator).to receive_message_chain(:new, :import)
        expect(RailsRedshiftReplicator::Importers::IdentityReplicator).not_to receive(:new)
        replicable.import
      end
    end
    context 'with last replication' do
      context 'and last replication is uploaded' do
        let!(:replication)  { create :redshift_replication, source_table: 'users', state: 'uploaded' }
        it 'calls import with the last uploaded replication' do
          allow(RailsRedshiftReplicator::Importers::IdentityReplicator).to receive_message_chain(:new, :import)
          expect(RailsRedshiftReplicator::Importers::IdentityReplicator).to receive(:new).with(replication)
          replicable.import
        end
      end
      context 'and last replication is imported' do
        it 'does not call import' do
          allow(RailsRedshiftReplicator::Importers::IdentityReplicator).to receive_message_chain(:new, :import)
          expect(RailsRedshiftReplicator::Importers::IdentityReplicator).not_to receive(:new)
          replicable.import
        end
      end
      context 'and last replication is importing' do
        let!(:replication)  { create :redshift_replication, source_table: 'users', state: 'importing' }
        context 'and max_retries is not set' do
          before { RailsRedshiftReplicator.max_retries = nil }
          it 'calls the correspondent importer class resuming the replication' do
            allow(RailsRedshiftReplicator::Importers::IdentityReplicator).to receive_message_chain(:new, :import)
            expect(RailsRedshiftReplicator::Importers::IdentityReplicator).to receive(:new).with(replication)
            replicable.import
          end
        end
        context 'and max_retries is set' do
          context "but wasn't reached" do
            before { RailsRedshiftReplicator.max_retries = 2 }
            it 'calls the correspondent importer class resuming the replication' do
              allow(RailsRedshiftReplicator::Importers::IdentityReplicator).to receive_message_chain(:new, :import)
              expect(RailsRedshiftReplicator::Importers::IdentityReplicator).to receive(:new).with(replication)
              replicable.import
            end
          end
          context 'and was reached' do
            before { RailsRedshiftReplicator.max_retries = 0 }
            it 'calls the correspondent importer class resuming the replication' do
              allow(RailsRedshiftReplicator::Importers::IdentityReplicator).to receive_message_chain(:new, :import)
              expect(RailsRedshiftReplicator::Importers::IdentityReplicator).not_to receive(:new).with(replication)
              replicable.import
            end
          end
        end
      end
    end
  end

  describe '#vacuum' do
    let(:replicable) { RailsRedshiftReplicator::Replicable.new(:identity_replicator, source_table: 'users', target_table: 'custom_users') }  
    it 'calls the central vacuum method' do
      expect(RailsRedshiftReplicator).to receive(:vacuum).with('custom_users')
      replicable.vacuum
    end
  end

  describe '#analyze' do
    let(:replicable) { RailsRedshiftReplicator::Replicable.new(:identity_replicator, source_table: 'users', target_table: 'custom_users') }  
    it 'calls the central vacuum method' do
      expect(RailsRedshiftReplicator).to receive(:analyze).with('custom_users')
      replicable.analyze
    end
  end



end
