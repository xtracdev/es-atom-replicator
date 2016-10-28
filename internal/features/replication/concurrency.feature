Feature: Replicator must acquire table lock to perform processing.
  Scenario:
    Given two replicators
    When both are started
    Then only one may execute the catch up logic

  Scenario:
    Given two replicators
    When start up is complete and steady state is reached
    And there are recent events
    Then only one replicator processes the recent events