package com.jionek.peopledb.repository;

import com.jionek.peopledb.exception.UnableToSaveException;

import java.sql.*;

abstract class CRUDRepository <T> {
    protected Connection connection;

    public CRUDRepository(Connection connection) {
        this.connection = connection;
    }

    public T save(T person) throws UnableToSaveException {
        try {
            PreparedStatement ps = connection.prepareStatement(getSaveSql(), Statement.RETURN_GENERATED_KEYS);
            mapForSave(person, ps);
            int recordsAffected = ps.executeUpdate();
            ResultSet rs = ps.getGeneratedKeys();
            while (rs.next()){
                long id = rs.getLong(1);
                person.setId(id);
                System.out.println(person);
            }
            System.out.printf("Records affected: %d%n", recordsAffected);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Tried to save person: " + person);
        }
        return person;
    }

    abstract void mapForSave(T entity, PreparedStatement ps) throws SQLException;

    abstract String getSaveSql();

    }
