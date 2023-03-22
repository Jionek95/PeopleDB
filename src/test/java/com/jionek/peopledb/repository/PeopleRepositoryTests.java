package com.jionek.peopledb.repository;

import com.jionek.peopledb.model.Person;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

public class PeopleRepositoryTests {

    private Connection connection;

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/crudapi", "root", "123456");
    }

    @Test
    public void canSaveOnePerson(){
        PeopleRepository repo = new PeopleRepository(connection);
        Person john = new Person(
                "John", "Smith", ZonedDateTime.of(1980, 11, 15 , 15,15,0,0, ZoneId.of("-6"))
        );
        Person savedPerson = repo.save(john);
        assertThat(savedPerson.getId()).isGreaterThan(0);
    }

   @Test
   public void canSaveTwoPeople(){
       PeopleRepository repo = new PeopleRepository(connection);
       Person john = new Person(
               "John", "Smith", ZonedDateTime.of(1980, 11, 15 , 15,15,0,0, ZoneId.of("-6"))
       );
       Person bobby = new Person(
               "Bobby", "Horse", ZonedDateTime.of(1985, 11, 25 , 15,15,12,0, ZoneId.of("-6"))
       );
       Person savedPerson1 = repo.save(john);
       Person savedPerson2 = repo.save(bobby);
       assertThat(savedPerson1.getId()).isNotEqualTo(savedPerson2.getId());
   }
}
