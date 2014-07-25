module Workflow
  module Adapter
    module ActiveRecordLocking
      def self.included(klass)
        klass.send :include, Adapter::ActiveRecordLocking::InstanceMethods
        klass.send :extend, Adapter::ActiveRecordLocking::Scopes
        klass.after_initialize :write_initial_state
      end

      module InstanceMethods
        def load_workflow_state
          read_attribute(self.class.workflow_column)
        end

        # On transition the new workflow state is immediately saved in the database.
        # Emulates state_machine by explicitly saving the entire record.
        def persist_workflow_state(new_value)
          column = self.class.workflow_column
          old_value = self[column]
          self[column] = new_value

          # If the record hasn't been created yet, ignore the locking logic
          # entirely and fall back to the previous behaviour.
          return save! if self.new_record?
          raise ActiveRecord::RecordInvalid.new(self) if invalid?

          lock_without_reload do
            rows = self.class.where(id: id, column => old_value)
                             .update_all(column => new_value)

            # TODO: Explictly allow two (a -> b) updates to happen simultaneously
            #       if 1) the transition is marked to allow this, and 2) the end
            #       result is the same.

            # Always explode if the UPDATE failed; it means when we've tried to
            # update state a -> state b, someone else has already beat us and
            # updated a -> c or something.
            raise ::ActiveRecord::StaleObjectError.new(self, 'experiment') if rows == 0

            # Explicitly go on to save the record normally; this is emulating
            # the normal state_machine transition behaviour, which everything
            # written up until now expects.
            # (Ideally this entire thing would be patterned after ActiveRecord::Locking::Implicit,
            # but that's basically impossible without monkey-patching AR directly.)
            save!
          end
        end

        private

        # Motivation: even if NULL is stored in the workflow_state database column,
        # the current_state is correctly recognized in the Ruby code. The problem
        # arises when you want to SELECT records filtering by the value of initial
        # state. That's why it is important to save the string with the name of the
        # initial state in all the new records.
        def write_initial_state
          write_attribute self.class.workflow_column, current_state.to_s if self[self.class.workflow_column].nil?
        end

        # ActiveRecord::Locking#lock unfortunately explicitly reloads the record
        # before going on to grab the lock. This avoids this.
        def lock_without_reload(&block)
          return unless block_given?

          # SELECT id FROM <table> WHERE id = <id> FOR UPDATE
          # Prevents any other process affect this row until the end of the
          table = self.class.arel_table
          self.class.connection.execute(
            table.from(table) # Bizarre, but directly from Arel's tests.
                 .project(:id) # SELECT id
                 .where(table[:id].eq(id))
                 .lock # Implicitly a FOR UPDATE lock.
                 .to_sql
          )
          yield
        end
      end

      # This module will automatically generate ActiveRecord scopes based on workflow states.
      # The name of each generated scope will be something like `with_<state_name>_state`
      #
      # Examples:
      #
      # Article.with_pending_state # => ActiveRecord::Relation
      #
      # Example above just adds `where(:state_column_name => 'pending')` to AR query and returns
      # ActiveRecord::Relation.
      module Scopes
        def self.extended(object)
          class << object
            alias_method :workflow_without_scopes, :workflow unless method_defined?(:workflow_without_scopes)
            alias_method :workflow, :workflow_with_scopes
          end
        end

        def workflow_with_scopes(&specification)
          workflow_without_scopes(&specification)
          states     = workflow_spec.states.values
          eigenclass = class << self; self; end

          states.each do |state|
            # Use eigenclass instead of `define_singleton_method`
            # to be compatible with Ruby 1.8+
            eigenclass.send(:define_method, "with_#{state}_state") do
              where("#{table_name}.#{self.workflow_column.to_sym} = ?", state.to_s)
            end
          end
        end
      end
    end
  end
end
