# encoding: UTF-8
# This file is auto-generated from the current state of the database. Instead
# of editing this file, please use the migrations feature of Active Record to
# incrementally modify your database, and then regenerate this schema definition.
#
# Note that this schema.rb definition is the authoritative source for your
# database schema. If you need to create the application database on another
# system, you should be using db:schema:load, not running all the migrations
# from scratch. The latter is a flawed and unsustainable approach (the more migrations
# you'll amass, the slower it'll run and the greater likelihood for issues).
#
# It's strongly recommended that you check this file into your version control system.

ActiveRecord::Schema.define(version: 20160509225445) do

  create_table "posts", force: :cascade do |t|
    t.integer  "user_id"
    t.text     "content"
    t.datetime "created_at"
    t.datetime "updated_at"
  end

  add_index "posts", ["user_id", "updated_at"], name: "index_posts_on_user_id_and_updated_at"

  create_table "rails_redshift_replicator_deleted_ids", id: false, force: :cascade do |t|
    t.string  "source_table"
    t.integer "object_id"
  end

  create_table "rails_redshift_replicator_replications", force: :cascade do |t|
    t.string   "replication_type"
    t.string   "key"
    t.string   "state",            default: "enqueued"
    t.string   "last_record"
    t.integer  "retries",          default: 0
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
    t.datetime "created_at",                            null: false
    t.datetime "updated_at",                            null: false
  end

  create_table "tags", force: :cascade do |t|
    t.string   "name"
    t.datetime "created_at"
    t.datetime "updated_at"
  end

  add_index "tags", ["name", "updated_at"], name: "index_tags_on_name_and_updated_at"

  create_table "tags_users", id: false, force: :cascade do |t|
    t.integer "user_id"
    t.integer "tag_id"
  end

  add_index "tags_users", ["user_id", "tag_id"], name: "index_tags_users_on_user_id_and_tag_id"

  create_table "users", force: :cascade do |t|
    t.string   "login"
    t.string   "password"
    t.integer  "age"
    t.boolean  "confirmed"
    t.datetime "created_at"
    t.datetime "updated_at"
  end

  add_index "users", ["login", "age"], name: "index_users_on_login_and_age"

  # no candidate create_trigger statement could be found, creating an adapter-specific one
  execute(<<-TRIGGERSQL)
CREATE TRIGGER posts_after_delete_row_tr AFTER DELETE ON "posts"
FOR EACH ROW
BEGIN
    INSERT INTO rails_redshift_replicator_deleted_ids(source_table, object_id) VALUES('posts', OLD.id);
END;
  TRIGGERSQL

  # no candidate create_trigger statement could be found, creating an adapter-specific one
  execute(<<-TRIGGERSQL)
CREATE TRIGGER tags_after_delete_row_tr AFTER DELETE ON "tags"
FOR EACH ROW
BEGIN
    INSERT INTO rails_redshift_replicator_deleted_ids(source_table, object_id) VALUES('tags', OLD.id);
END;
  TRIGGERSQL

  # no candidate create_trigger statement could be found, creating an adapter-specific one
  execute(<<-TRIGGERSQL)
CREATE TRIGGER users_after_delete_row_tr AFTER DELETE ON "users"
FOR EACH ROW
BEGIN
    INSERT INTO rails_redshift_replicator_deleted_ids(source_table, object_id) VALUES('users', OLD.id);
END;
  TRIGGERSQL

end
