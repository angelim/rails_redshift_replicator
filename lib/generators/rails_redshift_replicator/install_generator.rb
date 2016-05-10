require 'rails/generators/base'
require 'securerandom'

module RailsRedshiftReplicator
  module Generators

    class InstallGenerator < Rails::Generators::Base
      source_root File.expand_path("../../templates", __FILE__)

      desc "Creates a RRR initializer and copy locale files to your application."

      def copy_initializer
        template "rails_redshift_replicator.rb", "config/initializers/rails_redshift_replicator.rb"
      end

      def copy_locale
        copy_file "../../../config/locales/rails_redshift_replicator.en.yml", "config/locales/rails_redshift_replicator.en.yml"
      end

      def rails_4?
        Rails::VERSION::MAJOR == 4
      end
    end
  end
end