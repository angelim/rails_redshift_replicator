require 'spec_helper'

describe RailsRedshiftReplicator::Deleter do
  let(:replicable)  { RailsRedshiftReplicator.replicables[:users] }
  let(:deleter)     { RailsRedshiftReplicator::Deleter.new(replicable) }
  describe '#purge_deleted' do
    before { 2.times { User.create } }
    it 'purge deleted records for the given table' do
      expect(deleter.deleted_ids.count).to eq 0
      User.first.destroy
      expect(deleter.deleted_ids.count).to eq 1
      deleter.purge_deleted
      expect(deleter.deleted_ids.count).to eq 0
    end
  end
  describe '#delete_on_target' do
    it 'calls redshift to delete' do
      expect(RailsRedshiftReplicator.connection).to receive(:exec).and_return(double("result", result_status: 1))
      deleter.delete_on_target
    end
  end
  describe '#handle_delete_propagation' do
    context 'when tracking deleted' do
      context 'and has deleted records' do
        before do
          allow(RailsRedshiftReplicator.connection).to receive(:exec)
          2.times { User.create }
          User.delete_all
        end
        it 'calls #delete_on_target' do
          expect(deleter).to receive(:delete_on_target)
          deleter.handle_delete_propagation
        end
        context 'if delete on target is success' do
          before { allow(RailsRedshiftReplicator.connection).to receive(:exec).and_return(double("result", result_status: 1)) }
          it 'calls #purge_deleted' do
            expect(deleter).to receive(:purge_deleted)
            deleter.handle_delete_propagation
          end
        end
        context 'if delete on target fails' do
          before { allow(RailsRedshiftReplicator.connection).to receive(:exec).and_return(double("result", result_status: 0)) }
          it 'does not call calls #purge_deleted and logs error' do
            expect(deleter).not_to receive(:purge_deleted)
            expect(RailsRedshiftReplicator.logger).to receive(:error)
            deleter.handle_delete_propagation
          end
        end
      end
      context 'and without deleted records' do
        it 'does not call #delete_on_target' do
          expect(deleter).not_to receive(:delete_on_target)
          deleter.handle_delete_propagation
        end
      end
    end
    context 'if not tracking deleted' do
      before { replicable.instance_variable_set("@tracking", false) }
      it 'does not call #delete_on_target' do
        expect(deleter).not_to receive(:delete_on_target)
        deleter.handle_delete_propagation
      end
    end
  end
  describe '#deleted_ids' do
    it 'returns array of deleted ids' do
      user = create :user
      id = user.id
      User.delete_all
      expect(deleter.deleted_ids).to eq [id.to_s]
    end
  end
  describe '#has_deleted_ids?' do
    context 'when there are deleted records' do
      before do
        2.times { User.create }
        User.delete_all
      end
      it 'returns true' do
        expect(deleter.has_deleted_ids?).to be_truthy
      end
    end
    context "when there aren't deleted records" do
      it 'returns false' do
        expect(deleter.has_deleted_ids?).to be_falsy
      end
    end
  end

end