package org.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.oss.driver.api.core.CqlSession;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.boot.SpringBootVersion;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Abstract base class for integration tests. Utilizes testcontainers to create
 * a Cassandra instance for testing. Instance is started before all integration
 * tests are run, and is stopped once all the integration tests have completed.
 * Note that part of the setup identifies which version of Spring Boot is being
 * used as the properties used by spring-data-cassandra are different depending
 * on the version of Boot.
 */
@SpringBootTest(classes = {
  SampleCassandraApplication.class
})
@Testcontainers
abstract class CassandraSimpleIntegrationTest {

  // Instead of using the @Container annotation, we use a static initializer
  // block so that the container is created and started at the beginning of
  // the test suite and is not stopped/restarted for each test class.
  protected static final CassandraContainer<?> cassandra;

  static final String CASSANDRA_DOCKER_IMAGE = "cassandra:3.11.2";
  static final String KEYSPACE_NAME = "test";

  // Property names used to configure spring-data-cassandra session.
  static final String PROP_CASSANDRA_CONTACT_POINTS;
  static final String PROP_CASSANDRA_KEYSPACE_NAME;
  static final String PROP_CASSANDRA_PORT;

  static {
    // The properties differ from Boot 2.x to 3.x (Spring Data Cassandra change)
    // Affects the application YAML/properties file as well.
    if (SpringBootVersion.getVersion().startsWith("3.")) {
      PROP_CASSANDRA_CONTACT_POINTS = "spring.cassandra.contact-points";
      PROP_CASSANDRA_KEYSPACE_NAME = "spring.cassandra.keyspace-name";
      PROP_CASSANDRA_PORT = "spring.cassandra.port";
    } else {
      PROP_CASSANDRA_CONTACT_POINTS = "spring.data.cassandra.contact-points";
      PROP_CASSANDRA_KEYSPACE_NAME = "spring.data.cassandra.keyspace-name";
      PROP_CASSANDRA_PORT = "spring.data.cassandra.port";
    }

    // Create the container.
    cassandra = new CassandraContainer<>(CASSANDRA_DOCKER_IMAGE)
      .withExposedPorts(9042);

    // And start it.
    cassandra.start();
  }

  @BeforeAll
  static void setupCassandraConnectionProperties() {
    // Configure spring-data-cassandra.
    System.setProperty(PROP_CASSANDRA_KEYSPACE_NAME, KEYSPACE_NAME);
    System.setProperty(PROP_CASSANDRA_CONTACT_POINTS, cassandra.getHost());
    System.setProperty(PROP_CASSANDRA_PORT, String.valueOf(cassandra.getMappedPort(9042)));

    // Older version of testcontainers-cassandra used the cassandra.getCluster()
    // method to get access to the cluster. That has been deprecated, and the newer
    // versions require the Cluster.builder() to be used. Here, we probe the container
    // for host and port details to create the Cluster instance.
    createKeyspace(Cluster.builder()
      .addContactPoint(cassandra.getHost())
      .withPort(cassandra.getMappedPort(9042))
      .build());

    // This is for debugging purposes and not actually required.
    try (CqlSession session = createSession()) {
      System.out.println("==SETUP CASSANDRA CONNECTION PROPERTIES==");
      System.out.printf("Contact Point: %s%n", cassandra.getContactPoint());
      System.out.printf("Host: %s%n", cassandra.getHost());
      System.out.printf("Local DC: %s%n", cassandra.getLocalDatacenter());
      System.out.printf("Port: %d%n", cassandra.getMappedPort(9042));
      System.out.println(session.getName());
      System.out.println(session.getMetadata().getClusterName().orElse("(cluster name not found)"));
      System.out.println(session.getMetadata().getTokenMap().get());
      System.out.println("=========================================");
    }
  }

  private static void createKeyspace(final Cluster cluster) {
    try (Session session = cluster.connect()) {
      session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE_NAME +
        " WITH replication = \n" +
        "{'class':'SimpleStrategy','replication_factor':'1'};");
    }
  }

  private static CqlSession createSession() {
    return CqlSession.builder()
      .addContactPoint(cassandra.getContactPoint())
      .withLocalDatacenter(cassandra.getLocalDatacenter())
      .build();
  }

}
