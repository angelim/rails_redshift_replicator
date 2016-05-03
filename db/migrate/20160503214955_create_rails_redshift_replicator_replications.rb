class CreateRailsRedshiftReplicatorReplications < ActiveRecord::Migration
  def change
    create_table :rails_redshift_replicator_replications do |t|
      t.string   "replication_type"
      t.string   "key"
      t.string   "state",            :default => "enqueued"
      t.integer  "last_record"
      t.text     "last_error"
      t.string   "source_table"
      t.string   "target_table"
      t.integer  "slices"
      t.integer  "first_record"
      t.integer  "record_count"
      t.string   "export_format"
      t.integer  "export_duration"
      t.integer  "upload_duration"
      t.integer  "import_duration"
      t.text     "ids_to_delete"
      t.datetime "created_at",                               :null => false
      t.datetime "updated_at",                               :null => false
    end
  end
end