ENV['RAILS_ENV'] ||= 'test'

require 'dotenv'
Dotenv.load File.expand_path("../dummy/.env", __FILE__)

require File.expand_path("../dummy/config/environment.rb", __FILE__)

require 'rspec/rails'
require 'rspec/autorun'
require 'factory_girl_rails'
require 'shoulda-matchers'
require 'climate_control'
require 'pry-byebug'
require 'timecop'

Rails.backtrace_cleaner.remove_silencers!

# Load support files
Dir["#{File.dirname(__FILE__)}/support/**/*.rb"].each { |f| require f }

RSpec.configure do |config|
 config.mock_with :rspec
 config.use_transactional_fixtures = true
 config.infer_base_class_for_anonymous_controllers = false
 config.order = "random"
 config.filter_run_excluding :broken => true
 config.include FactoryGirl::Syntax::Methods
 config.include RailsRedshiftReplicatorHelpers
end

Shoulda::Matchers.configure do |config|
  config.integrate do |with|
    with.test_framework :rspec
    with.library :rails
  end
end