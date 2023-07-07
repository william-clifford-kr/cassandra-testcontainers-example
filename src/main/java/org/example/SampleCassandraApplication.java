package org.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;

/**
 * Application class for our Spring web application. Nothing fancy here, just the use
 * of the {@link EnableCassandraRepositories} annotation which triggers the
 * autoconfiguration for spring-data-cassandra. This requires the name of the package
 * where our Cassandra repositories are defined.
 */
@SpringBootApplication
@EnableCassandraRepositories(basePackages = {
  "org.example.dao"
})
public class SampleCassandraApplication {

  public static void main(final String[] args) {
    SpringApplication.run(SampleCassandraApplication.class, args);
  }

}
