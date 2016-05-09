require 'spec_helper'
describe RailsRedshiftReplicator::Tools::Vacuum do
  describe ".vacuum" do
    context "with :all" do
      let(:sql) { "VACUUM ;" }
      it "performs vacuum on all tables" do
        expect(RailsRedshiftReplicator.connection).to receive(:exec).with(sql)
        RailsRedshiftReplicator.vacuum(:all)
      end
    end
    context "without tables" do
      let(:sql) { "VACUUM ;" }
      it "performs vacuum on all tables" do
        expect(RailsRedshiftReplicator.connection).to receive(:exec).with(sql)
        RailsRedshiftReplicator.vacuum
      end
    end
    context "when using auto_tune" do
      it "perfoms vacuum on all tables" do
        expect_any_instance_of(RailsRedshiftReplicator::Tools::Vacuum).to receive(:auto_tuned_vacuum).with(:posts)
        RailsRedshiftReplicator.vacuum(:posts, "auto_tune")
      end
    end
  end
 
  describe ".auto_tuned_vacuum" do
    before do
      allow_any_instance_of(RailsRedshiftReplicator::Tools::Vacuum)
        .to receive(:sort_types)
        .and_return( [{"sort_type"=>"compound", "tablename"=>"users"}, {"sort_type"=>"interleaved", "tablename"=>"posts"}])
    end
    context "using :all" do
      let(:vacuum) { RailsRedshiftReplicator::Tools::Vacuum.new(:all) }
      it "performs vacuum on all tables" do
        expect(vacuum).to receive(:exec_vacuum_command).with("users", "compound")
        expect(vacuum).to receive(:exec_vacuum_command).with("posts", "interleaved")
        vacuum.auto_tuned_vacuum(:all)
      end
    end
    context "with a given table" do
      let(:vacuum) { RailsRedshiftReplicator::Tools::Vacuum.new(:users) }
      it "performs vacuum on the given table" do
        expect(vacuum).to receive(:exec_vacuum_command).with("users", "compound")
        expect(vacuum).not_to receive(:exec_vacuum_command).with("posts", "interleaved")
        vacuum.auto_tuned_vacuum("users")
      end
    end
  end

  describe "exec_vacuum_command" do
    let(:vacuum) { RailsRedshiftReplicator::Tools::Vacuum.new(:all) }
    context "with a compound key table" do
      it "builds the command" do
        expect(RailsRedshiftReplicator.connection).to receive(:exec).with("VACUUM FULL users;")
        vacuum.exec_vacuum_command("users", "compound")
      end
    end
    context "with an interleaved key table" do
      it "builds the command" do
        expect(RailsRedshiftReplicator.connection).to receive(:exec).with("VACUUM REINDEX posts;")
        vacuum.exec_vacuum_command("posts", "interleaved")
      end
    end
  end
end