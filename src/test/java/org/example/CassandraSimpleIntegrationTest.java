package org.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.oss.driver.api.core.CqlSession;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.boot.SpringBootVersion;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest(classes = {
    SampleCassandraApplication.class
})
@Testcontainers
abstract class CassandraSimpleIntegrationTest {

  //    @Container
  protected static final CassandraContainer<?> cassandra;
  static final String CASSANDRA_DOCKER_IMAGE = "cassandra:3.11.2";
  static final String KEYSPACE_NAME = "test";
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

    cassandra = new CassandraContainer<>(CASSANDRA_DOCKER_IMAGE).withExposedPorts(9042);
    cassandra.start();
  }

  @BeforeAll
  static void setupCassandraConnectionProperties() {
    System.setProperty(PROP_CASSANDRA_KEYSPACE_NAME, KEYSPACE_NAME);
    System.setProperty(PROP_CASSANDRA_CONTACT_POINTS, cassandra.getHost());
    System.setProperty(PROP_CASSANDRA_PORT, String.valueOf(cassandra.getMappedPort(9042)));
//        cassandra.getCluster();
    createKeyspace(Cluster.builder()
        .addContactPoint(cassandra.getHost())
        .withPort(cassandra.getMappedPort(9042))
        .build());
    try (CqlSession session = CqlSession.builder()
        .addContactPoint(cassandra.getContactPoint())
        .withLocalDatacenter(cassandra.getLocalDatacenter())
        .build()) {
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

}
