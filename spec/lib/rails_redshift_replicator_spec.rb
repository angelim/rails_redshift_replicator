require 'spec_helper'
class Mylogger < Logger;end
describe RailsRedshiftReplicator, type: :redshift_replicator do

  describe "Config" do
    %w(redshift_connection_params aws_credentials s3_bucket_params redshift_slices local_replication_path debug_mode history_cap).each do |param|
      it "responde para configuração #{param}" do
        expect(RailsRedshiftReplicator).to respond_to param
      end
    end
  end

  describe '.add_replicatable' do
    let(:replicatable) { RailsRedshiftReplicator::Replicatable.new(:identity_replicator, source_table: :users) }
    before { RailsRedshiftReplicator.replicatables = {}}
    it 'keeps a list of replicable tables' do
      RailsRedshiftReplicator.add_replicatable({ users: replicatable })
      expect(RailsRedshiftReplicator.replicatables.keys).to contain_exactly(:users)
    end
  end

  describe ".setup" do
    context 'with default configuration' do
      let(:env) do
        {
          RRR_REDSHIFT_HOST: 'default-host',
          RRR_REDSHIFT_DATABASE: 'default-database',
          RRR_REDSHIFT_PORT: 'default-port',
          RRR_REDSHIFT_USER: 'default-user',
          RRR_REDSHIFT_PASSWORD: 'default-password',
          RRR_AWS_ACCESS_KEY_ID: 'default-key',
          RRR_AWS_SECRET_ACCESS_KEY: 'default-secret',
          RRR_REPLICATION_REGION: 'default-region',
          RRR_REPLICATION_BUCKET: 'default-bucket'
        }
      end
      around do |example|
        ClimateControl.modify(env) do
          reset_config
          example.run
        end
      end
      it 'uses default redshift_connection_params' do
        expect(RailsRedshiftReplicator.redshift_connection_params).to eq({
          host: 'default-host',
          dbname: 'default-database',
          port: 'default-port',
          user: 'default-user',
          password: 'default-password'
        })
      end

      it 'uses default aws_credentials' do
        expect(RailsRedshiftReplicator.aws_credentials).to eq({
          key: 'default-key',
          secret: 'default-secret'
        })
      end

      it 'uses default s3_bucket_params' do
        expect(RailsRedshiftReplicator.s3_bucket_params).to eq({
          region: 'default-region',
          bucket: 'default-bucket',
          prefix: 'default-prefix'
        })
      end

      it 'uses default redshift_slices' do
        expect(RailsRedshiftReplicator.redshift_slices).to eq 1
      end

      it 'uses default local_replication_path' do
        expect(RailsRedshiftReplicator.local_replication_path).to eq '/tmp'
      end

      it 'uses default debug_mode' do
        expect(RailsRedshiftReplicator.debug_mode).to be_falsy
      end

      it 'uses default history_cap' do
        expect(RailsRedshiftReplicator.history_cap).to be_nil
      end
    end
    context 'when changing configuration' do
      before do
        RailsRedshiftReplicator.setup do |config|
          config.redshift_connection_params = {
            host: 'redshift-host',
            dbname: 'database-name',
            port: 'database-port',
            user: 'database-user',
            password: 'database-password'
          }

          config.aws_credentials = {
            key: 'aws-key',
            secret: 'aws-secret'
          }

          config.s3_bucket_params = {
            region: 's3-region',
            bucket: 's3-bucket'
          }

          config.redshift_slices = '2'
          config.local_replication_path = '/local-tmp'
          config.debug_mode = true
          config.history_cap = 3
          config.logger = Mylogger.new(STDOUT)
        end
      end

      it 'defines custom logger' do
        expect(RailsRedshiftReplicator.logger).to be_a Mylogger
      end
      
      it 'defines redshift_connection_params' do
        expect(RailsRedshiftReplicator.redshift_connection_params).to eq({
          host: 'redshift-host',
          dbname: 'database-name',
          port: 'database-port',
          user: 'database-user',
          password: 'database-password'
        })
      end

      it 'defines aws_credentials' do
        expect(RailsRedshiftReplicator.aws_credentials).to eq({
          key: 'aws-key',
          secret: 'aws-secret'
        })
      end

      it 'defines s3_bucket_params' do
        expect(RailsRedshiftReplicator.s3_bucket_params).to eq({
          region: 's3-region',
          bucket: 's3-bucket'
        })
      end

      it 'defines redshift_slices' do
        expect(RailsRedshiftReplicator.redshift_slices).to eq '2'
      end

      it 'defines local_replication_path' do
        expect(RailsRedshiftReplicator.local_replication_path).to eq '/local-tmp'
      end

      it 'defines debug_mode' do
        expect(RailsRedshiftReplicator.debug_mode).to eq true
        expect(RailsRedshiftReplicator.logger.level).to eq Logger::DEBUG
      end

      it 'defines history_cap' do
        expect(RailsRedshiftReplicator.history_cap).to eq 3
      end

      it "changes module's configuration parameters" do
        expect { RailsRedshiftReplicator.setup {|config| config.redshift_slices = 10} }.to change(RailsRedshiftReplicator, :redshift_slices).to(10)
      end
    end
  end

  describe ".connection" do
    it "retorna uma conexão para o redshift" do
      expect(RailsRedshiftReplicator.connection).to be_instance_of PG::Connection
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

  describe ".export" do
    it "calls Exporter::Base.export with params" do
      expect(RailsRedshiftReplicator::Exporter::Base).to receive(:export).with(:user, :publication)
      RailsRedshiftReplicator.export :user, :publication
    end
  end

  describe ".import" do
    it "calls Importer::Base.import with params" do
      expect(RailsRedshiftReplicator::Importer::Base).to receive(:import).with(:users, :downloads)
      RailsRedshiftReplicator.import :users, :downloads
    end
  end
end
