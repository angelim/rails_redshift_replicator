$:.push File.expand_path("../lib", __FILE__)

# Maintain your gem's version:
require "rails_redshift_replicator/version"

# Describe your gem and declare its dependencies:
Gem::Specification.new do |s|
  s.name        = "rails_redshift_replicator"
  s.version     = RailsRedshiftReplicator::VERSION
  s.authors     = ["Alexandre Angelim"]
  s.email       = ["angelim@angelim.com.br"]
  s.homepage    = "https://github.com/angelim/rails_redshift_replicator"
  s.summary     = "Replicate your ActiveRecord tables to Redshift"
  s.description = "Replicate your ActiveRecord tables to Redshift"
  s.license     = "MIT"

  s.files = Dir["{app,config,db,lib}/**/*", "MIT-LICENSE", "Rakefile", "README.rdoc"]
  s.test_files = Dir["spec/**/*"]

  s.add_dependency "rails", "~> 4.0"
  s.add_dependency "pg", '~> 0.18'
  s.add_dependency "activerecord-redshift-adapter"
  s.add_dependency "fog"

  s.add_development_dependency "sqlite3", '~> 1.3'
  s.add_development_dependency 'mysql2', '~> 0.3.16'
  s.add_development_dependency 'rspec-rails', '~> 3.4'
  s.add_development_dependency 'factory_girl_rails','~> 4.5'
  s.add_development_dependency 'shoulda-matchers', '~> 3.1'
  s.add_development_dependency 'pry-byebug'
  s.add_development_dependency 'dotenv-rails'
  s.add_development_dependency 'climate_control'
  s.add_development_dependency 'timecop'
end
