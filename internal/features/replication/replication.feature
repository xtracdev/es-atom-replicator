Feature: Feed replication
  Scenario:
    Given a replicator
    And an empty replication db
    When I process events
    Then the first feed page is replicated