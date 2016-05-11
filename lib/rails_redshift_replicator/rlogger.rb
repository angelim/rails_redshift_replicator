module RailsRedshiftReplicator
  class RLogger < ::Logger
    # Overrides logger methods to notify subscribers
    %w(info warn debug error).each do |severity|
      define_method severity do |message|
        ActiveSupport::Notifications.instrument('rails_redshift_replicator', type: severity.to_sym, message: message) do
          super message
        end
      end
    end
  end
end