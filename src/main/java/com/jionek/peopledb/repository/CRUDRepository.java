package com.jionek.peopledb.repository;

import com.jionek.peopledb.exception.UnableToSaveException;
import com.jionek.peopledb.model.Entity;
import com.jionek.peopledb.model.Person;

import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;

abstract class CRUDRepository <T extends Entity> {
    protected Connection connection;

    public CRUDRepository(Connection connection) {
        this.connection = connection;
    }

    public T save(T entity) throws UnableToSaveException {
        try {
            PreparedStatement ps = connection.prepareStatement(getSaveSql(), Statement.RETURN_GENERATED_KEYS);
            mapForSave(entity, ps);
            int recordsAffected = ps.executeUpdate();
            ResultSet rs = ps.getGeneratedKeys();
            while (rs.next()){
                long id = rs.getLong(1);
                entity.setId(id);
                System.out.println(entity);
            }
            System.out.printf("Records affected: %d%n", recordsAffected);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Tried to save person: " + entity);
        }
        return entity;
    }

    public Optional<Person> findById(Person person) {

        try {
            PreparedStatement ps = connection.prepareStatement(FIND_BY_ID_SQL);
            ps.setLong(1, person.getId());
            ResultSet rs = ps.executeQuery();
            while (rs.next()){
                long personId = rs.getLong("ID");
                String firstName = rs.getString("FIRST_NAME");
                String lastName = rs.getString("LAST_NAME");
                ZonedDateTime dob = ZonedDateTime.of(rs.getTimestamp("DOB").toLocalDateTime(), ZoneId.of("+0"));
                person = new Person(firstName, lastName, dob);
                person.setId(personId);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return Optional.ofNullable(person);
    }

    abstract void mapForSave(T entity, PreparedStatement ps) throws SQLException;

    abstract String getSaveSql();

    }
