module RailsRedshiftReplicator
  class Replication < ActiveRecord::Base
    STATES = %w(enqueued exporting exported uploading uploaded importing imported canceled)
    FORMATS = %w(gzip csv)

    # @return [Array] ids from source_table to delete on the next replication.
    serialize :ids_to_delete, Array

    validates :state, inclusion: { in: STATES }
    validates :export_format, inclusion: { in: FORMATS }
    validates_presence_of :replication_type, :key, :source_table, :target_table
    before_validation :setup_target_table

    # Clears the error column
    def clear_errors!
      update_attributes last_error: nil
    end

    # If replication is on an error state
    # @return [true, false] if has error
    def error?
      last_error.present?
    end

    # Initializes target table if it is blank
    def setup_target_table
      self.target_table = source_table if target_table.blank?
    end

    # Cancels the replication
    def cancel!
      update_attribute :state, 'canceled'
    end

    # @return [RailsRedshiftReplicator::Replicable] replicable for this model/table
    def replicable
      RailsRedshiftReplicator.replicables[source_table]
    end
    
    scope :from_table, ->(table) { where(source_table: Array(table).map(&:to_s)).where.not(state: 'canceled') }
    scope :with_state, ->(state) { where(state: state) }
    scope :older_than, ->(table, cap) { where("id < ?", cap_id(table, cap)) }

    def self.cap_id(table, cap)
      return unless cap
      where(source_table: table).order("id desc").limit(cap).pluck(:id)[cap-1]
    end

    # Builds helper methods to identify export format.
    # @return [true, false] if export is in a given format.
    FORMATS.each do |format|
      # @example
      #   self.format = "gzip"
      #   self.csv? #=> false
      define_method "#{format}?" do
        export_format == format
      end
    end

    STATES.each do |state|
      # Builds methods to change replication to a given state persisting changes.
      # @example
      #   self.uploaded! upload_duration: 10
      #   self.state #=> "uploaded"
      #   self.upload_duration #=> 10
      # @return [Time] current time
      define_method "#{state}!" do |options = {}|
        update_attributes({ state: state }.merge(options))
        return Time.now
      end

      # Builds methods to change replication to a given state _without_ persisting changes.
      # @example
      #   self.new_record? = true
      #   self.uploaded upload_duration: 10
      #   self.state #=> "uploaded"
      #   self.upload_duration #=> 10
      # @return [Time] current time
      define_method "#{state}" do |options = {}|
        assign_attributes({ state: state }.merge(options))
        return Time.now
      end

      # Builds helper methods to identify the current state.
      # @example
      #   self.state = "uploaded"
      #   self.uploaded? #=> true
      define_method "#{state}?" do
        self.state == state
      end

      # Scopes
      # @example
      #   scope :error, -> { where state: "error" }
      scope state, -> { where state: state }
    end
  end
end
