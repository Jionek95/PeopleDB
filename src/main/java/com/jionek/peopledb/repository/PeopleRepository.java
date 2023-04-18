package com.jionek.peopledb.repository;

import com.jionek.peopledb.annotation.SQL;
import com.jionek.peopledb.model.Address;
import com.jionek.peopledb.model.CrudOperation;
import com.jionek.peopledb.model.Person;
import com.jionek.peopledb.model.Region;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;


public class PeopleRepository extends CrudRepository<Person> {

    private AddressRepository addressRepository;
    public static final String SAVE_PERSON_SQL = """
            INSERT INTO PEOPLE
            (FIRST_NAME, LAST_NAME, DOB, SALARY, EMAIL, HOME_ADDRESS, BUSINESS_ADDRESS, SPOUSE)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)""";
    public static final String FIND_BY_ID_SQL = """
            SELECT
            P.ID AS P_ID, P.FIRST_NAME AS P_FIRST_NAME, P.LAST_NAME AS P_LAST_NAME, P.DOB AS P_DOB, P.SALARY AS P_SALARY,
            P.HOME_ADDRESS AS P_HOME_ADDRESS, P.BUSINESS_ADDRESS AS P_BUSINESS_ADDRESS, P.SPOUSE AS P_SPOUSE,
            
            S.ID AS S_ID, S.FIRST_NAME AS S_FIRST_NAME, S.LAST_NAME AS S_LAST_NAME, S.DOB AS S_DOB, S.SALARY AS S_SALARY,
            S.HOME_ADDRESS AS S_HOME_ADDRESS, S.BUSINESS_ADDRESS AS S_BUSINESS_ADDRESS, S.SPOUSE AS S_SPOUSE,
            
            HOME.ID AS HOME_ID, HOME.STREET_ADDRESS AS HOME_STREET_ADDRESS, HOME.ADDRESS2 AS HOME_ADDRESS2, HOME.CITY AS HOME_CITY, HOME.STATE AS HOME_STATE, HOME.POSTCODE AS HOME_POSTCODE, HOME.COUNTY AS HOME_COUNTY, HOME.REGION AS HOME_REGION, HOME.COUNTRY AS HOME_COUNTRY,
            BUSINESS.ID AS BUSINESS_ID, BUSINESS.STREET_ADDRESS AS BUSINESS_STREET_ADDRESS, BUSINESS.ADDRESS2 AS BUSINESS_ADDRESS2, BUSINESS.CITY AS BUSINESS_CITY, BUSINESS.STATE AS BUSINESS_STATE, BUSINESS.POSTCODE AS BUSINESS_POSTCODE, BUSINESS.COUNTY AS BUSINESS_COUNTY, BUSINESS.REGION AS BUSINESS_REGION, BUSINESS.COUNTRY AS BUSINESS_COUNTRY
            
            FROM PEOPLE AS P
            LEFT JOIN ADDRESSES AS HOME ON P.HOME_ADDRESS = HOME.ID
            LEFT JOIN ADDRESSES AS BUSINESS ON P.BUSINESS_ADDRESS = BUSINESS.ID
            
            LEFT JOIN PEOPLE AS S ON P.SPOUSE = S.ID
            
            WHERE P.ID=?
            """;
    public static final String FIND_ALL_SQL = "SELECT * FROM PEOPLE";
    public static final String SELECT_COUNT_SQL = "SELECT COUNT(*) FROM PEOPLE";
    public static final String DELETE_SQL = "DELETE FROM PEOPLE WHERE ID=?";
    public static final String DELETE_IN_SQL = "DELETE FROM PEOPLE WHERE ID IN(:ids)";
    public static final String UPDATE_SQL = "UPDATE PEOPLE SET FIRST_NAME=?, LAST_NAME=?, DOB=?, SALARY=? WHERE ID=?";

    public PeopleRepository(Connection connection) {
        super(connection);
        addressRepository = new AddressRepository(connection);
    }


    @Override
    @SQL(value = SAVE_PERSON_SQL, operationType = CrudOperation.SAVE)
    void mapForSave(Person entity, PreparedStatement ps) throws SQLException {
        Address savedAddress;
        ps.setString(1, entity.getFirstName());
        ps.setString(2, entity.getLastName());
        ps.setTimestamp(3, convertDobToTimestamp(entity.getDob()));
        ps.setBigDecimal(4, entity.getSalary());
        ps.setString(5, entity.getEmail());
        asssociateAddressWithEntity(6, ps, entity.getHomeAddress());
        asssociateAddressWithEntity(7, ps, entity.getBusinessAddress());
    }

    private void asssociateAddressWithEntity(int parameterIndex, PreparedStatement ps, Optional<Address> address) throws SQLException {
        Address savedAddress;
        if (address.isPresent()) {
            savedAddress = addressRepository.save(address.get());
            ps.setLong(parameterIndex, savedAddress.id());
        } else {
            ps.setObject(parameterIndex, null);
        }
    }

    @Override
    @SQL(value = UPDATE_SQL, operationType = CrudOperation.UPDATE)
    void mapForUpdate(Person entity, PreparedStatement ps) throws SQLException {
        ps.setString(1, entity.getFirstName());
        ps.setString(2, entity.getLastName());
        ps.setTimestamp(3, convertDobToTimestamp(entity.getDob()));
        ps.setBigDecimal(4, entity.getSalary());
    }

    @Override
    @SQL(value = FIND_BY_ID_SQL, operationType = CrudOperation.FIND_BY_ID)
    @SQL(value = FIND_ALL_SQL, operationType = CrudOperation.FIND_ALL)
    @SQL(value = SELECT_COUNT_SQL, operationType = CrudOperation.COUNT)
    @SQL(value = DELETE_SQL, operationType = CrudOperation.DELETE_ONE)
    @SQL(value = DELETE_IN_SQL, operationType = CrudOperation.DELETE_MANY)
    Person extractEntityFromResultSet(ResultSet rs) throws SQLException {
        long personId = rs.getLong("ID");
        String firstName = rs.getString("FIRST_NAME");
        String lastName = rs.getString("LAST_NAME");
        ZonedDateTime dob = ZonedDateTime.of(rs.getTimestamp("DOB").toLocalDateTime(), ZoneId.of("+0"));
        BigDecimal salary = rs.getBigDecimal("SALARY");
        long homeAddressId = rs.getLong("HOME_ADDRESS");
        long businessAddressId = rs.getLong("BUSINESS_ADDRESS");

        Address homeAddress = extractAddress(rs, "HOME_");
        Address businessAddress = extractAddress(rs, "BUSINESS_");
        Person person = new Person(personId, firstName, lastName, dob, salary);

        person.setHomeAddress(homeAddress);
        person.setBusinessAddress(businessAddress);
        return person;
    }

    private Address extractAddress(ResultSet rs, String aliasPrefix) throws SQLException {
//        if (rs.getObject("HOME_ID") == null) return null;
//        long addressId = rs.getLong("HOME_ID");

        Long adrId = getValueByAlias(aliasPrefix + "ID", rs, Long.class);
        if (adrId == null) return null;
        String streetAddress = getValueByAlias(aliasPrefix + "STREET_ADDRESS", rs, String.class);
        String address2 = getValueByAlias(aliasPrefix + "ADDRESS2", rs, String.class);
        String city = getValueByAlias(aliasPrefix + "CITY", rs, String.class);
        String state = getValueByAlias(aliasPrefix + "STATE", rs, String.class);
        String postcode = getValueByAlias(aliasPrefix + "POSTCODE", rs, String.class);
        String county = getValueByAlias(aliasPrefix + "COUNTY", rs, java.lang.String.class);
        Region region = Region.valueOf(getValueByAlias(aliasPrefix + "REGION", rs, String.class).toUpperCase());
        String country = getValueByAlias(aliasPrefix + "COUNTRY", rs, String.class);
        Address address = new Address(adrId, streetAddress, address2, city, state, postcode, county, region, country);

//        Address address = new Address(addressId, streetAddress, address2, city, state, postcode, county, region, country);

        return address;
    }

    // Can be used in general way
    private <T> T getValueByAlias(String alias, ResultSet rs, Class<T> clazz) throws SQLException {
        int columnCount = rs.getMetaData().getColumnCount();
        for(int colIdx=1; colIdx<=columnCount; colIdx++){
            if (alias.equals(rs.getMetaData().getColumnLabel(colIdx))){
               return (T) rs.getObject(colIdx);
            }
        }
        throw  new SQLException(String.format("Column not found for alias: '%s'", alias));
    }


    private static Timestamp convertDobToTimestamp(ZonedDateTime dob) {
        return Timestamp.valueOf(dob.withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime());
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


    /** SLOWER VERSION OF delete(Person...people)
//    public void delete(Person...people) {
//        for(Person person : people){
//            delete(person);
//        }
//    }
     **/



}


