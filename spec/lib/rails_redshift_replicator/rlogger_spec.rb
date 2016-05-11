require 'spec_helper'

describe RailsRedshiftReplicator::RLogger do
  @messages = []
  let!(:logger) { RailsRedshiftReplicator::RLogger.new(STDOUT) }
  it 'emits info notification' do
    expect(ActiveSupport::Notifications).to receive(:instrument).with('rails_redshift_replicator', type: :info, message: 'hello')
    logger.info 'hello'
  end
  it 'emits warn notification' do
    expect(ActiveSupport::Notifications).to receive(:instrument).with('rails_redshift_replicator', type: :warn, message: 'hello')
    logger.warn 'hello'
  end
  it 'emits debug notification' do
    expect(ActiveSupport::Notifications).to receive(:instrument).with('rails_redshift_replicator', type: :debug, message: 'hello')
    logger.debug 'hello'
  end
  it 'emits error notification' do
    expect(ActiveSupport::Notifications).to receive(:instrument).with('rails_redshift_replicator', type: :error, message: 'hello')
    logger.error 'hello'
  end
end
