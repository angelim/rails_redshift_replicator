module RailsRedshiftReplicator
  class Engine < ::Rails::Engine
    isolate_namespace RailsRedshiftReplicator
    config.generators do |g|
      g.test_framework :rspec, :fixture => false
      g.fixture_replacement :factory_girl, :dir => 'spec/factories'
    end
  end
end
