Feature: Health check
  Scenario:
    Given a database
    And healch check endpont is enabled
    When I call health check endpont
    Then the response indicates the replicator is healthy