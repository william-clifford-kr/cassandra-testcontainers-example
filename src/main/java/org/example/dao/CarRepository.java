package org.example.dao;

import java.util.UUID;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface CarRepository extends CassandraRepository<Car, UUID> {

}
