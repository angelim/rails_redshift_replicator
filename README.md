# RailsRedshiftReplicator(RRR)
Replicate your Rails app tables to Redshift.

There are several reasons one should choose to replicate data to Redshift. Most of them are related to Data Analysis or Reporting activities.
Maybe you're using tools like [Snowplow](https://github.com/snowplow/snowplow) and loading events data directly into Redshift, but still needs to enrich those at runtime with your resources tables. Maybe you have a huge amount of data on your resources tables and were asked to build some complex reports. You may have even been tracking events on a local database and realized data analysis is too heavy for some database engines. 

Replicating data to Redshift can be a [real pain](http://docs.aws.amazon.com/redshift/latest/dg/t_Loading_tables_with_the_COPY_command.html). If you're already on AWS ecosystem there are some [Data Pipeline templates](http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-template-redshift.html) that can help you with that, but even those don't provide finer level of control.

**RRR** was built to perform incremental replication from any ActiveRecord compatible database to Redshift. It allows replicating just a subset of fields, target table renaming, keeping track of hard deletes on source and using different replication strategies depending on your use case. Those features are not covered by the Data Pipeline templates provided by Amazon.

**This gem is not meant to perform Real Time Synchronization between your database and Redshift.** There are some paid services out there to perform that kind of replication.

## Installation

Add it to your Gemfile with:

```ruby
gem 'rails_redshift_replicator'
```

Run the bundle command to install it.

Install the configuration template and copy migrations with:

```bash
rails generate rails_redshift_replicator:install
rake rails_redshift_replicator:install:migrations
```
Run the migrations and take a look at `config/initializers/rails_redshift_replicator.rb` for some [Configuration](#configuration) examples.

## Usage

In order to start using RRR, your tables must be defined on Redshift. RRR will export data using that definition, so you don't have to 
worry about column ordering.

RRR comes with 3 replication strategies out of the box.
- __TimedReplicator__: Probably the most common replication strategy. Uses a timestamp field to select which records to export and merge with the ones already on Redshift.
- __IdentityReplicator__: Intended for immutable tables, where you don't expect/allow updates to happen(eg. events table). It's faster than the TimedReplicator strategy because it justs adds records to Redshift instead of merging.
- __FullReplicator__: Performs a full table export and import on every replication. Intended for habtm tables that don't have an identity column.

It's important to note that RRR will only replicate the columns that are defined on the **Redshift table**. That way you can avoid copying sensitive or even useless information for your purposes.

On your models:

```ruby
class Event
  belongs_to :user

  # In this example, 'events' is an immutable table. Once an event is recorded, it can never be updated.
  # This is a perfect example for an identity_replicator, that will only look for increments on the 'id'(or other identity column) column
  # and won't bother looking to merge updated records on Redshift.
  has_redshift_replication :identity_replicator
end

class User
  has_many :posts
  has_and_belongs_to_many :tags

  # In this example, the 'users' table is mutable and frequently updated. Using the timed_replicator will update
  # records on Redshift since the last replication along with adding the new ones.
  has_redshift_replication :timed_replicator
end

class Tag
  has_and_belongs_to_many :users

  # Maybe the 'tags' table is already taken on Redshift, so we must retarget the replication to another table: `custom_tags`.
  has_redshift_replication :timed_replicator, target_table: :custom_tags, replication_field: :created_at
end

# Sometimes you won't have a model mapped to a table you wan't to replicate.
# That may be the case with several vanilla habtm tables. For those you must define the replication manually, like the example bellow.
# config/initializers/rails_redshift_replicator.rb
RailsRedshiftReplicator::Replicable.new :full_replicator, source_table: 'tags_users'

```

Lets take a look at those options:

| Option | Description |
| --- | --- |
| source_table | Name of the local table. Defaults to ActiveRecord mapping. Essencialy you'll only use this outside of an ActiveRecord model |
| target_table | Name of the table on Redshift. Defaults to `source_table` |
| replication_field | Name of the field which will be used to calculate incremental replication. Defaults to `updated_at` on TimedReplicator and `id` on IdentityReplicator |
| enable_delete_tracking | If the table deletes should be monitored for later replication |


### Performing Replications

The most practical way of performing a replication is directly through the RailsRedshiftReplicator Module.

```ruby
# Replicate all registered replicable tables:
RailsRedshiftReplicator.replicate(:all)

# Replicate a table or tables(Use the source table as argument):
RailsRedshiftReplicator.replicate(:users, :posts)
  # or
User.rrr_replicate

# Run a particular replication process for all tables
RailsRedshiftReplicator.export(:all)
RailsRedshiftReplicator.import(:all)

# Run a particular replication process for specific tables:
RailsRedshiftReplicator.export(:users, :posts)
RailsRedshiftReplicator.import(:users, :posts)
  # or
User.rrr_export
User.rrr_import

```

RRR will filter out tables that are not registered for replication and log them for you. It will also wrap the import stage in a transaction and it is automatically rolled back if any problem arrises.


## Configuration

Most of the configuration can be done with environment variables(at least the sensitive ones). This configuration template will be copied to your `config/initializers/rails_redshift_replicator.rb`. If you don't like the ENV variables approach you can expose your credentials on this file, but it is strongly recommended that you remove it from your repository.

```ruby
RailsRedshiftReplicator.setup do |config|

  # RRR already provides a logger pointing to STDOUT, but you can point it to your own logger.
  # Just be sure to make it inherit from RailsRedshiftReplicator::RLogger or you will loose
  # the notifications feature.
  config.logger = MyLogger.new

  # Connection parameters for Redshift. Defaults to environment variables.
  config.redshift_connection_params = {
    host: ENV['RRR_REDSHIFT_HOST'],
    dbname: ENV['RRR_REDSHIFT_DATABASE'],
    port: ENV['RRR_REDSHIFT_PORT'],
    user: ENV['RRR_REDSHIFT_USER'],
    password: ENV['RRR_REDSHIFT_PASSWORD']
  }

  # AWS S3 Replication bucket credentials. Defaults to environment variables.
  config.aws_credentials = {
    key: ENV['RRR_AWS_ACCESS_KEY_ID'],
    secret: ENV['RRR_AWS_SECRET_ACCESS_KEY']
  }

  # AWS S3 replication bucket parameters.
  # region defaults to environment variable or US East (N. Virginia)
  # bucket defaults to environment variable
  config.s3_bucket_params = {
    region: (ENV['RRR_REPLICATION_REGION'] || 'us-east-1'),
    bucket: ENV['RRR_REPLICATION_BUCKET'],
    prefix: ENV['RRR_REPLICATION_PREFIX']
  }

  # Number of slices available on Redshift cluster. Used to split export files. Defaults to 1.
  # see [http://docs.aws.amazon.com/redshift/latest/dg/t_splitting-data-files.html]
  config.redshift_slices = 1

  # Folder to store temporary replication files until the S3 upload. Defaults to /tmp
  config.local_replication_path = '/tmp'

  # Command or path to executable that splits files
  config.split_command = 'split'

  # Command or path to executable that compresses files to gzip
  config.gzip_command = 'gzip'

  # Enable debug mode to output messages to STDOUT. Default to false
  config.debug_mode = false

  # Defines how many replication records are kept in history. Default to nil keeping full history.
  config.history_cap = nil

  # Defines how many errors are allowed to happen when importing into Redshfit
  # see [http://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-load.html#copy-maxerror]
  config.max_copy_errors = 0

  # Preferred format for export file
  config.preferred_format = 'csv'

  # Maximum number of retries for a replication before cancelling and starting another
  config.max_retries = 5

  # If deletes should be tracked and propagated to redshift
  # Take a look at the "A word on tracking deletions" section
  config.enable_delete_tracking = false
end
```

## The replication model

The history of replications is kept in a table created during install. That table has an ActiveRecord model `RailsRedshiftReplicator::Replication` to make it easier to explore and manipulate.

That model is used to keep the replication state, information required to perform the replications incrementally and error messages generated during the process.

If you don't want to keep the whole history, you can configure RRR to purge the oldest replication records with:

```ruby 
# Keep only the 10 most recent replication records.
RailsRedshiftReplicator.setup { |config| config.history_cap = 10 } 

```

## Replication Retries

If for any reason a replication fails, RRR will retry it on the next replication process. The default configuration is to allow 5 retries, but you can specify something else on a configuration block. RRR won't perform a whole new replication until an incomplete one has been flagged as imported or canceled.

The behavior of the retry strategy is as follows:
If the replication stopped at (enqueued, exporting, exported, uploading) states, it exports the data again and then import to Redshift
If the replication stopped at (uploaded importing) states, it skips the exporting process and performs the import directly.
A replication is automaticaly canceled if it exceeds the maximum number of allowed retries. In that case, a new replication record is created.

#### Helper methods for Replication

```ruby
# Export output format testing
replication.csv? #=> true

# Changing state with additional properties
replication.update!(import_duration: 10) #=> DateTime

# Replication state testing
replication.uploaded? #=> false

# Scopes for source tables
Replication.from_table(:users) #=> ActiveRecord::Relation

# Scopes for states
Replication.with_state(:imported) #=> ActiveRecord::Relation


```

## Notifications

You can subscribe to receive replication notifications using [ActiveSupport Notifications](http://api.rubyonrails.org/classes/ActiveSupport/Notifications.html).

```ruby
ActiveSupport::Notifications.subscribe('rails_redshift_replicator') do |name, start, finish, id, payload|
  # payload[:type] can be (:info, :debug, :error)
  if payload[:type] == :error
    # Send email to the boss.
    puts payload[:message]
  end
end

```

## A word on tracking deletions

Tracking deletions can be somewhat cumbersome for `identity_replicator` and `timed_replicator` strategies.
The easiest way to keep a complete set of records up to date is to setup your model with some hard deletion protection
using gems like [Paranoia](https://github.com/rubysherpas/paranoia) or [ActsAsParanoid](https://github.com/ActsAsParanoid/acts_as_paranoid). Protecting records from hard deletion is advantageous for scenarios where you need to keep resource references for reporting or analysis history.

Hard deletions are "hard" to track on ActiveRecord because not all destructing methods go through the callback chain(eg: Model.delete_all). That means we can't reliably use callbacks to hook up a method to keep track of those deletions. Delegating that responsibility to developers would be too intrusive and error-prone.

If you really need to discard deleted records and to replicate those deletions, RRR provides a mechanism to do so with **database triggers**. This mechanism requires maintenance and some level of discipline that should be taken into account.

During the instalation process, RRR creates a very simple table to hold deleted records. If you enable tracking for deleted records globally or for a given table, RRR will create trackers using the excellent [HairTrigger gem](https://github.com/jenseng/hair_trigger).
That will spare you from the effort of understanding the trigger creation syntax for your particular ActiveRecord database flavor. Those trigger definitions, however, require a migration to take effect, just like any schema change. After adding or removing RRR to a model you must use the following commands:

```bash
rake db:generate_trigger_migration
rake db:migrate
```
The first method will create the migration either to create or drop triggers. It's smart enough to just generate migratons when you add or remove RRR to a table.

Those triggers will populate the deletions table whenever a record is deleted, irrespective of the method you use to do so. During the replication process those deletes are propagated to Redshift. 

**As of now, only tables with an `id` column are supported.**


## Memory bloat protection

Sometimes replications deal with A LOT or records. Using regular AR queries to dump data to the CSV file would take a lot of memory and could potentially crash your app. RRR uses specific driver features to stream data instead of deserialize and instantiate everything.
That feature is only enabled for the Mysql and PostgreSQL drivers.


## Helper methods for ActiveRecord models

```ruby

# Replicate model
User.rrr_replicate

# Only export model
User.rrr_export

# Only import model
User.rrr_import

# Perform Redshift's VACUUM command on target table
User.rrr_vacuum

# Perform Redshift's ANALYZE command on target table
User.rrr_analyze

# Perform deleted records propagation

User.rrr_deleter.handle_delete_propagation # propagates deletions to Redshift

```

## Tools

Amazon provides some tools to keep your tables clean and performant. It's only fair RRR helps you do that.

**VACUUM**
[Reclaims space](http://docs.aws.amazon.com/redshift/latest/dg/r_VACUUM_command.html) and resorts rows in either a specified table or all tables in the current database.

Particularly, the `timed_replicator` strategy first deletes the updated records from the target table and then reinserts them during the replication. Redshift doesn't reclaim space during that process. You must perfom a VACUUM command to do so.

The vacuum command uses different options based on the type of SORT KEYS used on your table, but not to worry. RRR already takes care of selecting the right options.

```ruby
RailsRedshiftReplicator.vacuum(:all)
RailsRedshiftReplicator.vacuum(:users, :posts)
#or
User.rrr_vacuum
Post.rrr_vacuum

```

**ANALYZE**
Updates table statistics for use by the query planner.
After adding a lot of new unsorted records, use this command to get your queries more performant.


```ruby
RailsRedshiftReplicator.analyze(:all)
RailsRedshiftReplicator.analyze(:users, :posts)
#or
User.analyze
Post.analyze

```

## Scheduling Replications

It's only natural that you'd like to schedule replications to run periodically. Be careful to not schedule them in an unfeasible interval, trying to create a new replication before the previous one has had time to finish. Run some by hand and take a look at the Replication history duration fields (#export_duration, #upload_duration and #import_duration) or even the difference between #updated_at and #created_at. Remember that if you try to run a new replication before the previous one finishes you will trigger a retry.

Schedule on cron using the Rails Runner:
```bash
crontab -e
# schedule for every hour
0 * * * * cd path/to/my/app && bundle exec rails runner "RailsRedshiftReplicator.replicate(:all)"
```
You can also create a bash script to make it more concise.

## Disclaimer
Please test this gem before using it on production. This project is still on it's infancy and hasn't been extensively tested in the real world. The rspec tests may not be comprehensive enough to take your use case or environment into account.

Try it on a test Redshift database before pointing it to your production tables. There isn't any code here that could affect your source tables, but writing to Redshift is a destructive process.

## How to contribute
TODO

