module RailsRedshiftReplicator
  class Engine < ::Rails::Engine
    isolate_namespace RailsRedshiftReplicator

    config.generators do |g|
      g.test_framework :rspec, :fixture => false
      g.fixture_replacement :factory_girl, :dir => 'spec/factories'
    end

    initializer "rrr.initialisation" do |app|
      # ActiveRecord::Base.send :include, RailsRedshiftReplicator::Model
    end
  end
end
