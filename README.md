# cassandra-testcontainers-example

Working example of Spring Data Cassandra that has been tested with both Spring Boot v2.7.13 and v3.0.8, based upon
sample code from (Baeldung)[https://www.baeldung.com/spring-data-cassandra-test-containers]. Dependency versions
have been updated from the example code, and a custom `HealthIndicator` which provides some extended details about
the Cassandra connection provided by Spring Data Cassandra has been added.

May or may not be adding to this - it is, for now, a basic example to test compatibility and show some of the data
available via Spring Data Cassandra. It is not meant to be "production code", but may be used for reference if found
to be useful to anyone. The application has been tested with:

* Spring Boot versions `2.7.13` and `3.0.8`; provide Gradle project property `spring.boot.version` to specify the
  version you wish to use. If left unspecified, the default is `2.7.13`.
* Java 17.0.6 from Liberica. Note Spring Boot and Gradle requirements for the compatible versions of Java.
* Gradle version `8.1.1`; should work with versions `6.x` and later, but not tested.
