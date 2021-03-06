# This migration comes from rails_redshift_replicator (originally 20160503214955)
# 20160503214955
class CreateRailsRedshiftReplicatorReplications < ActiveRecord::Migration
  def change
    create_table :rails_redshift_replicator_replications do |t|
      t.string   "replication_type"
      t.string   "key"
      t.string   "state",            :default => "enqueued"
      t.string   "last_record"
      t.integer  "retries", default: 0
      t.text     "last_error"
      t.string   "source_table"
      t.string   "target_table"
      t.integer  "slices"
      t.string   "first_record"
      t.integer  "record_count"
      t.string   "export_format"
      t.integer  "export_duration"
      t.integer  "upload_duration"
      t.integer  "import_duration"
      t.datetime "created_at",                               :null => false
      t.datetime "updated_at",                               :null => false
    end
  end
end