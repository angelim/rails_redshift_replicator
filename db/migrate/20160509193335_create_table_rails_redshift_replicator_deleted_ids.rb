class CreateTableRailsRedshiftReplicatorDeletedIds < ActiveRecord::Migration
  def change
    create_table :rails_redshift_replicator_deleted_ids, id: false do |t|
      t.string :source_table
      t.integer :object_id
    end
  end
end
