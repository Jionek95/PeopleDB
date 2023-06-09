package com.jionek.peopledb.repository;

import com.jionek.peopledb.annotation.Id;
import com.jionek.peopledb.annotation.MultiSql;
import com.jionek.peopledb.annotation.SQL;
import com.jionek.peopledb.exception.DataException;
import com.jionek.peopledb.exception.UnableToSaveException;
import com.jionek.peopledb.model.CrudOperation;

import java.sql.*;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

abstract class CrudRepository<T> {
    protected Connection connection;
    private PreparedStatement savePS;

    public CrudRepository(Connection connection) {
        try {
            this.connection = connection;
            savePS = connection.prepareStatement(getSqlByAnnotation(CrudOperation.SAVE, this::getSaveSql), Statement.RETURN_GENERATED_KEYS);
        } catch (SQLException e) {
            throw new DataException("Unable to create prepared statement for CrudRepository", e);
        }
    }


    public T save(T entity) throws UnableToSaveException {
        try {
            mapForSave(entity, savePS);
            int recordsAffected = savePS.executeUpdate();
            ResultSet rs = savePS.getGeneratedKeys();
            while (rs.next()){
                long id = rs.getLong(1);
                setIdByAnnotation(id, entity);
                postSave(entity, id);
            }
//            System.out.printf("Records affected: %d%n", recordsAffected);
        } catch (SQLException e) {
            e.printStackTrace();
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
            PreparedStatement ps = connection.prepareStatement(
                    getSqlByAnnotation(CrudOperation.FIND_ALL, this::getFindAllSql),
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
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
            ps.setLong(1, getIdByAnnotation(entity));
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
                    .map(entity -> getIdByAnnotation(entity))
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
            ps.setLong(5, getIdByAnnotation(entity));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String getSqlByAnnotation(CrudOperation operationType, Supplier<String> sqlGetter){
        Stream<SQL> multiSqlStream = Arrays.stream(this.getClass().getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(MultiSql.class))
                .map(method -> method.getAnnotation(MultiSql.class))
                .flatMap(multiSql -> Arrays.stream(multiSql.value()));

        Stream<SQL> sqlStream = Arrays.stream(this.getClass().getDeclaredMethods())
//                .filter(method -> methodName.contentEquals(method.getName()))
                .filter(method -> method.isAnnotationPresent(SQL.class))
                .map(method -> method.getAnnotation(SQL.class));

        return Stream.concat(multiSqlStream, sqlStream)
                .filter(annotation -> annotation.operationType().equals(operationType))
                .map(SQL::value)
                .findFirst().orElseGet(sqlGetter);
    }

    private void setIdByAnnotation(Long id, T entity){
        Arrays.stream(entity.getClass().getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(Id.class))
                .forEach(field -> {
                    field.setAccessible(true);
                    try {
                        field.set(entity, id);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException("Unable to set ID field value");
                    }
                });
    }

    private Long getIdByAnnotation(T entity){
        return Arrays.stream(entity.getClass().getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(Id.class))
//                .map(field -> field.getAnnotation(Id.class))
                .map(field -> {
                    field.setAccessible(true);
                    Long id = null;
                    try {
                        id = (long) field.get(entity);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                    return id;
                })
                .findFirst().orElseThrow(() -> new RuntimeException("No ID annotated field founded."));
    }

    /**
     *
     * @return Should return SQL String like:
     * "DELETE FROM PEOPLE WHERE ID IN(:ids)"
     */
    protected String getDeleteInSql(){throw new RuntimeException("SQL not defined.");}
    protected String getDeleteSql(){throw new RuntimeException("SQL not defined.");}
    protected String getCountSql(){throw new RuntimeException("SQL not defined.");}
    protected String getFindAllSql(){throw new RuntimeException("SQL not defined.");}
    protected String getSaveSql(){throw new RuntimeException("SQL not defined.");}
    protected String getUpdateSql(){throw new RuntimeException("SQL not defined.");}
    /**
     *
     * @return Returns a String that represents the SQL needed to retrieve on entity
     * The SQL must contain one SQL parameter, i.e. "?" that will bind to the entity's ID.
     */
    protected String getfindByIdSql(){throw new RuntimeException("SQL not defined.");}
    protected void postSave(T entity, long id) { }


    abstract T extractEntityFromResultSet(ResultSet rs) throws SQLException;
    abstract void mapForSave(T entity, PreparedStatement ps) throws SQLException;
    abstract void mapForUpdate(T entity, PreparedStatement ps) throws SQLException;

}
