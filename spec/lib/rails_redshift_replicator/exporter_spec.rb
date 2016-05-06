require 'spec_helper'

describe RailsRedshiftReplicator::Exporter do
  let(:exporter) { RailsRedshiftReplicator::Exporter.new } 
  let(:user_replicatable)  { RailsRedshiftReplicator::Replicatable.new(:identity_replicator, source_table: :users)}
  let(:post_replicatable)  { RailsRedshiftReplicator::Replicatable.new(:timed_replicator, source_table: :posts)}
  let(:habtm_replicatable) { RailsRedshiftReplicator::Replicatable.new(:full_replicator, source_table: :tags_users)}
  
  before do
    RailsRedshiftReplicator.add_replicatable({ "users" => user_replicatable})
    RailsRedshiftReplicator.add_replicatable({ "posts" => post_replicatable})
    RailsRedshiftReplicator.add_replicatable({ "tags_users" => habtm_replicatable})
  end

  describe '#tables_to_export' do
    context 'when exporting all tables' do
      it 'returns all tables' do
        expect(exporter.tables_to_export(:all)).to contain_exactly('users', 'posts', 'tags_users')
      end
    end
    context 'when exporting selected tables' do
      context 'and all given tables are replicatable' do
        it 'returns all given tables' do
          expect(exporter.tables_to_export(:users, :posts)).to contain_exactly('users', 'posts')
        end
      end
      context 'and some given tables are not replicatable' do
        it 'returns only the tables that are eligible for replication' do
          expect(exporter.tables_to_export(:users, :posts, :tags)).to contain_exactly('users','posts')
        end
      end
    end
  end
  describe '#perform' do
    context 'with one table' do
      it 'exports records from the given table' do
        exporter = RailsRedshiftReplicator::Exporter.new(:users)
        expect_any_instance_of(RailsRedshiftReplicator::Exporters::IdentityReplicator).to receive(:export_and_upload)
        exporter.perform
      end
    end
    context 'with all tables' do
      it 'exports records for all eligible tables' do
        exporter = RailsRedshiftReplicator::Exporter.new(:all)
        expect_any_instance_of(RailsRedshiftReplicator::Exporters::IdentityReplicator).to receive(:export_and_upload)
        expect_any_instance_of(RailsRedshiftReplicator::Exporters::TimedReplicator).to receive(:export_and_upload)
        expect_any_instance_of(RailsRedshiftReplicator::Exporters::FullReplicator).to receive(:export_and_upload)
        exporter.perform
      end
    end
  end
end