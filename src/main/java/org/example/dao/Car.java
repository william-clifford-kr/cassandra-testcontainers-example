package org.example.dao;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

import java.util.UUID;

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
