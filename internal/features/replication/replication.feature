Feature: Feed replication
  Scenario:
    Given a replicator
    And an empty replication db
    When I process events
    Then the first feed page is replicated

  Scenario:
    Given a replicator
    And new events to replicate
    When the latest aggregate reference is not in the source
    And I replicate
    Then I pick up the new events anyway