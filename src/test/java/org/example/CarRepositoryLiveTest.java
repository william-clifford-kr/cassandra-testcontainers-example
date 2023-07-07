package org.example;

import com.datastax.driver.core.utils.UUIDs;
import org.example.dao.Car;
import org.example.dao.CarRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

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
