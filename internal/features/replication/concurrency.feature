@concurrency
Feature: Replicator must acquire table lock to perform processing.
  Scenario:
    Given two replicators
    And no events have been replicated
    When both are started
    Then only one may execute the catch up logic
    And the first feed page is replicated
