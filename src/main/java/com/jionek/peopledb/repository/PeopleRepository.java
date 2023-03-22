package com.jionek.peopledb.repository;

import com.jionek.peopledb.model.Person;

import java.sql.Connection;

public class PeopleRepository {
    private Connection connection;
    public PeopleRepository(Connection connection) {
        this.connection = connection;
    }

    public Person save(Person person) {
        String sql = "INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB) VALUES";
        connection.prepareStatement(sql);
        return person;
    }
}
