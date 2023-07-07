package org.example.dao;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

/**
 * Spring data repository.
 */
@Repository
public interface CarRepository extends CassandraRepository<Car, UUID> {

}
