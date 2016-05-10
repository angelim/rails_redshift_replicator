# This migration was auto-generated via `rake db:generate_trigger_migration'.
# While you can edit this file, any changes you make to the definitions here
# will be undone by the next auto-generated trigger migration.

class CreateTriggersPostsDeleteOrTagsDeleteOrUsersDelete < ActiveRecord::Migration
  def up
    create_trigger("posts_after_delete_row_tr", :generated => true, :compatibility => 1).
        on("posts").
        after(:delete) do
      "INSERT INTO rails_redshift_replicator_deleted_ids(source_table, object_id) VALUES('posts', OLD.id);"
    end

    create_trigger("tags_after_delete_row_tr", :generated => true, :compatibility => 1).
        on("tags").
        after(:delete) do
      "INSERT INTO rails_redshift_replicator_deleted_ids(source_table, object_id) VALUES('tags', OLD.id);"
    end

    create_trigger("users_after_delete_row_tr", :generated => true, :compatibility => 1).
        on("users").
        after(:delete) do
      "INSERT INTO rails_redshift_replicator_deleted_ids(source_table, object_id) VALUES('users', OLD.id);"
    end
  end

  def down
    drop_trigger("posts_after_delete_row_tr", "posts", :generated => true)

    drop_trigger("tags_after_delete_row_tr", "tags", :generated => true)

    drop_trigger("users_after_delete_row_tr", "users", :generated => true)
  end
end
