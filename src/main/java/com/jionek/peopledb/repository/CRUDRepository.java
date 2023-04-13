package com.jionek.peopledb.repository;

import com.jionek.peopledb.annotation.SQL;
import com.jionek.peopledb.exception.UnableToSaveException;
import com.jionek.peopledb.model.Entity;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.joining;

abstract class CRUDRepository <T extends Entity> {
    protected Connection connection;

    public CRUDRepository(Connection connection) {
        this.connection = connection;
    }


    private String getSaveSqlByAnnotation(){
        return Arrays.stream(this.getClass().getDeclaredMethods())
                .filter(method -> "mapForSave".contentEquals(method.getName()))
                .map(method -> method.getAnnotation(SQL.class))
                .map(SQL::value)
                .findFirst().orElse(getSaveSql());
    }

    public T save(T entity) throws UnableToSaveException {
        try {
            PreparedStatement ps = connection.prepareStatement(getSaveSqlByAnnotation(), Statement.RETURN_GENERATED_KEYS);
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

    public Optional<T> findById(Long id ) {
        T entity = null;

        try {
            PreparedStatement ps = connection.prepareStatement(getfindByIdSql());
            ps.setLong(1, id);
            ResultSet rs = ps.executeQuery();
            while (rs.next()){
                entity = extractEntityFromResultSet(rs);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return Optional.ofNullable(entity);
    }

    public List<T> findAll() {
        List<T> entities = new ArrayList<>();

        try {
            PreparedStatement ps = connection.prepareStatement(getFindAllSql());
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                entities.add(extractEntityFromResultSet(rs));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return entities;
    }

    public long count() {
        long count = 0;

        try {
            PreparedStatement ps = connection.prepareStatement(getCountSql());
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                count = rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return count;
    }

    public void delete(T entity) {
        try {
            PreparedStatement ps = connection.prepareStatement(getDeleteSQL());
            ps.setLong(1, entity.getId());
            int recordsAffected = ps.executeUpdate();
            System.out.println(recordsAffected);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void delete(T... entities) {
        try {
            Statement cs = connection.createStatement();

            String ids = Arrays.stream(entities)
                    .map(entity -> entity.getId())
                    .map(id -> String.valueOf(id))
                    .collect(joining(","));

            int affectedRecordCount = cs.executeUpdate(getDeleteInSql().replace(":ids", ids));// :ids is a named parameter
            System.out.println(affectedRecordCount);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void update(T entity) {
        try {
            PreparedStatement ps = connection.prepareStatement(getUpdateSQL());
            mapForUpdate(entity, ps);
            ps.setLong(5, entity.getId());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract String getUpdateSQL();

    /**
     *
     * @return Should return SQL String like:
     * "DELETE FROM PEOPLE WHERE ID IN(:ids)"
     */
    protected abstract String getDeleteInSql();
    protected abstract String getDeleteSQL();
    protected abstract String getCountSql();
    protected abstract String getFindAllSql();
    abstract T extractEntityFromResultSet(ResultSet rs) throws SQLException;

    /**
     *
     * @return Returns a String that represents the SQL needed to retrieve on entity
     * The SQL must contain one SQL parameter, i.e. "?" that will bind to the entity's ID.
     */
    abstract String getfindByIdSql();
    abstract void mapForSave(T entity, PreparedStatement ps) throws SQLException;
    abstract void mapForUpdate(T entity, PreparedStatement ps) throws SQLException;
    String getSaveSql(){return "";}

    }
