class User < ActiveRecord::Base
  has_redshift_replication :identity_replicator
  has_and_belongs_to_many :tags
  has_many :posts
end