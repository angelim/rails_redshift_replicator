class Post < ActiveRecord::Base
  has_redshift_replication :timed_replicator
  belongs_to :user
end