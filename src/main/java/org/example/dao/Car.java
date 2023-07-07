package org.example.dao;

import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

@AllArgsConstructor
@Data
@Table
public class Car {

    @PrimaryKey
    private UUID id;

    private String make;

    private String model;

    private int year;

}
