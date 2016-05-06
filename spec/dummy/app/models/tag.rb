class Tag < ActiveRecord::Base
  has_redshift_replication :timed_replicator, target_table: :custom_tags, replication_field: :created_at
  has_and_belongs_to_many :users
end