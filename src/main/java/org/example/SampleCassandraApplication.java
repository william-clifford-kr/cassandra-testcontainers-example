package org.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;

@SpringBootApplication
@EnableCassandraRepositories(basePackages = {
  "org.example.dao"
})
public class SampleCassandraApplication {

  public static void main(final String[] args) {
    SpringApplication.run(SampleCassandraApplication.class, args);
  }

}
