module RailsRedshiftReplicator
  class RLogger < ::Logger
    CHANNEL = 'rails_redshift_replicator'
    def info(message)
      ActiveSupport::Notifications.instrument(CHANNEL, type: :info, message: message) do
        super message
      end
    end

    def debug(message)
      ActiveSupport::Notifications.instrument(CHANNEL, type: :debug, message: message) do
        super message
      end
    end

    def error(message)
      ActiveSupport::Notifications.instrument(CHANNEL, type: :error, message: message) do
        super message
      end
    end
  end
end