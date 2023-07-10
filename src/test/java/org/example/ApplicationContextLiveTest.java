package org.example;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ApplicationContextLiveTest extends CassandraSimpleIntegrationTest {

  @Test
  void givenCassandraContainer_whenSpringContextIsBootstrapped_thenContainerIsRunningWithNoExceptions() {
    assertThat(cassandra.isRunning()).isTrue();
  }

}
