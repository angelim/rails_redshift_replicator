require 'spec_helper'

describe RailsRedshiftReplicator do
  describe "Config" do
    %w(redshift_connection_params aws_credentials s3_bucket_params redshift_slices local_replication_path debug_mode history_cap).each do |param|
      it "responde para configuração #{param}" do
        expect(RailsRedshiftReplicator).to respond_to param
      end
    end
  end

  describe '.add_replicable', broken: true do
    let(:replicable) { RailsRedshiftReplicator::Replicable.new(:identity_replicator, source_table: :users) }
    around do |example|
      old = RailsRedshiftReplicator.replicables
      RailsRedshiftReplicator.replicables = {}
      example.run
      RailsRedshiftReplicator.replicables = old
    end
    it 'keeps a list of replicable tables' do
      RailsRedshiftReplicator.add_replicable({ users: replicable })
      expect(RailsRedshiftReplicator.replicables.keys).to contain_exactly(:users)
    end
  end

  describe ".connection" do
    it "retorna uma conexão para o redshift" do
      expect(RailsRedshiftReplicator.connection).to be_instance_of PG::Connection
    end
  end

  describe '#tables_to_perform' do
    context 'when exporting all tables' do
      before { RailsRedshiftReplicator.add_replicable(tags_users: RailsRedshiftReplicator::Replicable.new(:full_replicator, source_table: :tags_users)) }
      it 'returns all tables' do
        expect(RailsRedshiftReplicator.tables_to_perform(:all)).to contain_exactly('users', 'posts', 'tags', 'tags_users')
      end
    end
    context 'when exporting selected tables' do
      context 'and all given tables are replicable' do
        it 'returns all given tables' do
          expect(RailsRedshiftReplicator.tables_to_perform([:users, :posts])).to contain_exactly('users', 'posts')
        end
      end
      context 'and some given tables are not replicable' do
        it 'returns only the tables that are eligible for replication' do
          expect(RailsRedshiftReplicator.tables_to_perform([:users, :posts, :nonrep])).to contain_exactly('users','posts')
        end
        it 'warns about unreplicable' do
          expect(RailsRedshiftReplicator).to receive(:warn_if_unreplicable).with(['nonrep'])
          RailsRedshiftReplicator.tables_to_perform([:users, :posts, :nonrep])
        end
      end
    end
  end
  describe '#check_args' do
    context 'when no tables were given' do
      it 'raises error' do
        expect{ RailsRedshiftReplicator.check_args([]) }.to raise_error(StandardError)
      end
    end
    context 'when tables were given' do
      it 'returns nil' do
        expect(RailsRedshiftReplicator.check_args([:users])).to be_nil
      end
    end
  end

  describe '.debug_mode=' do
    context 'enabling debug_mode' do
      before { RailsRedshiftReplicator.debug_mode = false }
      it 'changes logger level to debug' do
        expect { RailsRedshiftReplicator.setup {|config| config.debug_mode = true} }.to change(RailsRedshiftReplicator.logger, :level).to(Logger::DEBUG)
      end
    end
    context 'disabling debug_mode' do
      before { RailsRedshiftReplicator.debug_mode = true }
      it 'changes logger level to error' do
        expect { RailsRedshiftReplicator.setup {|config| config.debug_mode = false} }.to change(RailsRedshiftReplicator.logger, :level).to(Logger::WARN)
      end
    end
  end
  describe '.history_cap=', :focus do
    context 'when nil' do
      it 'assigns nil' do
        RailsRedshiftReplicator.history_cap = nil
        expect(RailsRedshiftReplicator.history_cap).to be_nil
      end
    end
    context 'when 2' do
      it 'assigns 2' do
        RailsRedshiftReplicator.history_cap = 2
        expect(RailsRedshiftReplicator.history_cap).to eq 2
      end
    end
    context 'when > 2' do
      it 'assigns given cap' do
        RailsRedshiftReplicator.history_cap = 5
        expect(RailsRedshiftReplicator.history_cap).to eq 5
      end
    end
    context 'when < 2' do
      it 'assigns 2' do
        RailsRedshiftReplicator.history_cap = 1
        expect(RailsRedshiftReplicator.history_cap).to eq 2
      end
    end
  end

end
