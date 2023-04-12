package com.jionek.peopledb.repository;

import com.jionek.peopledb.model.Person;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.joining;

public class PeopleRepository extends CRUDRepository<Person> {

    public static final String SAVE_PERSON_SQL = "INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB) VALUES(?, ?, ?)";
    public static final String FIND_BY_ID_SQL = "SELECT ID, FIRST_NAME, LAST_NAME, DOB, SALARY FROM PEOPLE WHERE ID=?";
    public static final String FIND_ALL_SQL = "SELECT * FROM PEOPLE";

    public PeopleRepository(Connection connection) {
        super(connection);
    }

    @Override
    String getSaveSql() {
        return SAVE_PERSON_SQL;
    }

    @Override
    void mapForSave(Person person, PreparedStatement ps) throws SQLException {
        ps.setString(1, person.getFirstName());
        ps.setString(2, person.getLastName());
        ps.setTimestamp(3, convertDobToTimestamp(person.getDob()));
    }

    @Override
    Person extractEntityFromResultSet(ResultSet rs) throws SQLException {
        long personId = rs.getLong("ID");
        String firstName = rs.getString("FIRST_NAME");
        String lastName = rs.getString("LAST_NAME");
        ZonedDateTime dob = ZonedDateTime.of(rs.getTimestamp("DOB").toLocalDateTime(), ZoneId.of("+0"));
        BigDecimal salary = rs.getBigDecimal("SALARY");
        return new Person(personId, firstName, lastName, dob, salary);
    }

    @Override
    String getfindByIdSql() {
        return FIND_BY_ID_SQL;
    }

    @Override
    protected String getFindAllSql() {
        return FIND_ALL_SQL;
    }

    /** OVERLOADED METHOD findById(Person person)
//    public Optional<Person> findById(Person person) {
//
//        try {
//            PreparedStatement ps = connection.prepareStatement(FIND_BY_ID_SQL);
//            ps.setLong(1, person.getId());
//            ResultSet rs = ps.executeQuery();
//            while (rs.next()) {
//                long personId = rs.getLong("ID");
//                String firstName = rs.getString("FIRST_NAME");
//                String lastName = rs.getString("LAST_NAME");
//                ZonedDateTime dob = ZonedDateTime.of(rs.getTimestamp("DOB").toLocalDateTime(), ZoneId.of("+0"));
//                person = new Person(firstName, lastName, dob);
//                person.setId(personId);
//            }
//
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//        return Optional.ofNullable(person);
//    }
     **/


    public List<Person> findAll() {
        List<Person> people = new ArrayList<>();

        try {
            PreparedStatement ps = connection.prepareStatement(FIND_ALL_SQL);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                people.add(extractEntityFromResultSet(rs));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return people;
    }


    public long count() {
        long count = 0;

        try {
            PreparedStatement ps = connection.prepareStatement("SELECT COUNT(*) FROM PEOPLE");
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                count = rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return count;
    }

    public void delete(Person person) {
        try {
            PreparedStatement ps = connection.prepareStatement("DELETE FROM PEOPLE WHERE ID=?");
            ps.setLong(1, person.getId());
            int recordsAffected = ps.executeUpdate();
            System.out.println(recordsAffected);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /** SLOWER VERSION OF delete(Person...people)
//    public void delete(Person...people) {
//        for(Person person : people){
//            delete(person);
//        }
//    }
     **/

    public void delete(Person... people) {
        try {
            Statement cs = connection.createStatement();

            String ids = Arrays.stream(people)
                    .map(person -> person.getId())
                    .map(id -> String.valueOf(id))
                    .collect(joining(","));

            int affectedRecordCount = cs.executeUpdate("DELETE FROM PEOPLE WHERE ID IN(:ids)".replace(":ids", ids));// :ids is a named parameter
            System.out.println(affectedRecordCount);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void update(Person person) {
        try {
            PreparedStatement ps = connection.prepareStatement("UPDATE PEOPLE SET FIRST_NAME=?, LAST_NAME=?, DOB=?, SALARY=? WHERE ID=?");
            ps.setString(1, person.getFirstName());
            ps.setString(2, person.getLastName());
            ps.setTimestamp(3, convertDobToTimestamp(person.getDob()));
            ps.setBigDecimal(4, person.getSalary());
            ps.setLong(5, person.getId());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static Timestamp convertDobToTimestamp(ZonedDateTime dob) {
        return Timestamp.valueOf(dob.withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime());
    }
}


