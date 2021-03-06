module RailsRedshiftReplicatorHelpers
  def with_modified_env(options = {}, &block)
    env = { REDSHIFT_HOST: '1',
      REDSHIFT_DATABASE: '2',
      REDSHIFT_PORT: '3',
      REDSHIFT_USER: '4',
      REDSHIFT_PASSWORD: '5',
      REDSHIFT_SLICES: '6',
      AWS_ACCESS_KEY_ID: '7',
      AWS_SECRET_ACCESS_KEY: '8',
      RDP_REPLICATION_REGION: '9',
      LOCAL_REPLICATION_PATH: '10',
      RDP_ANALYTICS_BUCKET: '11'}.merge(options)
    ClimateControl.modify(env, &block)
  end

  def replication_file(name)
    File.open("spec/support/csv/#{name}")
  end

  # Changes ActiveRecord connection to Redshift for the duration of the example
  # and restablishes connection to the original database right afterwards.
  def with_engine_connection
    original = ActiveRecord::Base.remove_connection
    ActiveRecord::Base.establish_connection(
      adapter: "redshift",
      host: RailsRedshiftReplicator.redshift_connection_params[:host],
      database: RailsRedshiftReplicator.redshift_connection_params[:dbname],
      port: RailsRedshiftReplicator.redshift_connection_params[:port],
      username: RailsRedshiftReplicator.redshift_connection_params[:user],
      password: RailsRedshiftReplicator.redshift_connection_params[:password])
    yield
  ensure
    ActiveRecord::Base.establish_connection(original)
  end

  # Recreates the test users table on redshift
  def recreate_users_table(custom_table_name = nil)
    table_name = custom_table_name || :users
    with_engine_connection do
      ActiveRecord::Migration.create_table table_name, force: true do |t|
        t.string :login
        t.integer :age
        t.boolean :confirmed
        t.timestamps
      end
    end
  end

  # Recreates the posts users table on redshift
  def recreate_posts_table(custom_table_name = nil)
    table_name = custom_table_name || :posts
    with_engine_connection do
      ActiveRecord::Migration.create_table table_name, force: true do |t|
        t.belongs_to :user
        t.text :content
        t.timestamps
      end
    end
  end

  # Recreates the test tags table on redshift
  def recreate_tags_table(custom_table_name = nil)
    table_name = custom_table_name || :tags
    with_engine_connection do
      ActiveRecord::Migration.create_table table_name, force: true do |t|
        t.string :name
        t.timestamps
      end
    end
  end

  # Recreates the test tags table on redshift
  def recreate_tags_users_table(custom_table_name = nil)
    table_name = custom_table_name || :tags_users
    with_engine_connection do
      ActiveRecord::Migration.create_table table_name, id: false, force: true do |t|
        t.integer :user_id
        t.integer :tag_id
      end
    end
  end

  # Drops a given test table on redshift
  # @param table [String,Symbol] test table's name
  def drop_redshift_table(table)
    with_engine_connection do
      begin
        ActiveRecord::Migration.drop_table table.to_s
      rescue => e
        puts "Coudn't drop table #{table}: #{e.message}"
      end
    end
  end
end
