Rails.application.routes.draw do

  mount RailsRedshiftReplicator::Engine => "/rails_redshift_replicator"
end
