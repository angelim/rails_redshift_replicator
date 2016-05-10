HairTrigger.class_eval do
  # includes triggers defined on replicables
  def self.current_triggers
    canonical_triggers = models.map(&:triggers).flatten.compact || []
    canonical_triggers += RailsRedshiftReplicator.replicables.values.map(&:triggers).flatten.compact
    canonical_triggers.each(&:prepare!) # interpolates any vars so we match the migrations
  end
end
