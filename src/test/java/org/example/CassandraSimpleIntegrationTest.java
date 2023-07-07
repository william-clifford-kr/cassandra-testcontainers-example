package org.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.oss.driver.api.core.CqlSession;
import org.example.dao.Car;
import org.example.dao.CarRepository;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootVersion;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = {
        SampleCassandraApplication.class
})
@Testcontainers
abstract class CassandraSimpleIntegrationTest {

    static final String CASSANDRA_DOCKER_IMAGE = "cassandra:3.11.2";
    static final String KEYSPACE_NAME = "test";

    static final String PROP_CASSANDRA_CONTACT_POINTS;
    static final String PROP_CASSANDRA_KEYSPACE_NAME;
    static final String PROP_CASSANDRA_PORT;

    //    @Container
    protected static final CassandraContainer<?> cassandra;

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

class ApplicationContextLiveTest extends CassandraSimpleIntegrationTest {

    @Test
    void givenCassandraContainer_whenSpringContextIsBootstrapped_thenContainerIsRunningWithNoExceptions() {
        assertThat(cassandra.isRunning()).isTrue();
    }

}

class CarRepositoryLiveTest extends CassandraSimpleIntegrationTest {

    @Autowired
    private CarRepository carRepository;

    @Test
    void givenValidCarRecord_whenSavingIt_thenRecordIsSaved() {
        final UUID carId = UUIDs.timeBased();
        final Car newCar = new Car(carId, "Nissan", "Qashqai", 2018);

        carRepository.save(newCar);

        final List<Car> savedCars = carRepository.findAllById(List.of(carId));
        assertThat(savedCars.get(0)).isEqualTo(newCar);
    }

    @Test
    void givenExistingCarRecord_whenUpdatingIt_thenRecordIsUpdated() {
        final UUID carId = UUIDs.timeBased();
        final Car existingCar = carRepository.save(new Car(carId, "Nissan", "Qashqai", 2018));

        existingCar.setModel("X-Trail");
        carRepository.save(existingCar);

        final List<Car> savedCars = carRepository.findAllById(List.of(carId));
        assertThat(savedCars.get(0).getModel()).isEqualTo("X-Trail");
    }

    @Test
    void givenExistingCarRecord_whenDeletingIt_thenRecordIsDeleted() {
        final UUID carId = UUIDs.timeBased();
        final Car existingCar = carRepository.save(new Car(carId, "Nissan", "Qashqai", 2018));

        carRepository.delete(existingCar);

        final List<Car> savedCars = carRepository.findAllById(List.of(carId));
        assertThat(savedCars).isEmpty();
    }

}
