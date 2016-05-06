# Migration 20160504120421
class CreateTestTables < ActiveRecord::Migration
  def up
    create_table :users do |t|
      t.string :login
      t.string :password
      t.integer :age
      t.boolean :confirmed
      t.timestamps
    end
    add_index :users, [:login, :age]
    
    create_table :posts do |t|
      t.belongs_to :user
      t.text :content
      t.timestamps
    end
    add_index :posts, [:user_id, :updated_at]
    
    create_table :tags do |t|
      t.string :name
      t.timestamps
    end
    add_index :tags, [:name, :updated_at]
    
    create_table :tags_users, id: false do |t|
      t.belongs_to :user
      t.belongs_to :tag
    end
    add_index :tags_users, [:user_id, :tag_id]

  end

  def down
    drop_table :users
    drop_table :posts
    drop_table :tags
    drop_table :tags_users
  end
end
