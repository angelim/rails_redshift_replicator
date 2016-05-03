FactoryGirl.define do
  factory :redshift_replication, class: 'RailsRedshiftReplicator::Replication' do
    export_format "csv"
    replication_type "IdReplicator"
    key "rdplibrary/replication/users/users_1442958786.csv"
    source_table "users"
  end
end
