package com.jionek.peopledb.repository;

import com.jionek.peopledb.annotation.SQL;
import com.jionek.peopledb.exception.UnableToSaveException;
import com.jionek.peopledb.model.CrudOperation;
import com.jionek.peopledb.model.Entity;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.stream.Collectors.joining;

abstract class CRUDRepository <T extends Entity> {
    protected Connection connection;

    public CRUDRepository(Connection connection) {
        this.connection = connection;
    }


    public T save(T entity) throws UnableToSaveException {
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.SAVE, this::getSaveSql), Statement.RETURN_GENERATED_KEYS);
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
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.FIND_BY_ID, this::getfindByIdSql));
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
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.FIND_ALL, this::getFindAllSql));
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
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.COUNT, this::getCountSql));
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
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.DELETE_ONE, this::getDeleteSql));
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

            int affectedRecordCount = cs.executeUpdate(getSqlByAnnotation(CrudOperation.DELETE_MANY, this::getDeleteInSql).replace(":ids", ids));// :ids is a named parameter
            System.out.println(affectedRecordCount);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    public void update(T entity) {
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.UPDATE, this::getUpdateSql));
            mapForUpdate(entity, ps);
            ps.setLong(5, entity.getId());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String getSqlByAnnotation(CrudOperation operationType, Supplier<String> sqlGetter){
        return Arrays.stream(this.getClass().getDeclaredMethods())
//                .filter(method -> methodName.contentEquals(method.getName()))
                .filter(method -> method.isAnnotationPresent(SQL.class))
                .map(method -> method.getAnnotation(SQL.class))
                .filter(annotation -> annotation.operationType().equals(operationType))
                .map(SQL::value)
                .findFirst().orElseGet(sqlGetter);
    }

    protected String getSaveSql(){return "";}
    protected String getUpdateSql(){return "";}

    /**
     *
     * @return Should return SQL String like:
     * "DELETE FROM PEOPLE WHERE ID IN(:ids)"
     */
    protected String getDeleteInSql(){return "";}
    protected String getDeleteSql(){return "";}
    protected String getCountSql(){return "";}
    protected String getFindAllSql(){return  "";}
    /**
     *
     * @return Returns a String that represents the SQL needed to retrieve on entity
     * The SQL must contain one SQL parameter, i.e. "?" that will bind to the entity's ID.
     */
    protected String getfindByIdSql(){return "";}

    abstract T extractEntityFromResultSet(ResultSet rs) throws SQLException;
    abstract void mapForSave(T entity, PreparedStatement ps) throws SQLException;
    abstract void mapForUpdate(T entity, PreparedStatement ps) throws SQLException;

}
