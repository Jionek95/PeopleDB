package com.jionek.peopledb.repository;

import com.jionek.peopledb.model.Address;
import com.jionek.peopledb.model.Person;
import com.jionek.peopledb.model.Region;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;

public class PeopleRepositoryTest {

    private Connection connection;
    private PeopleRepository repo;


    @BeforeEach
    void setUp() throws SQLException {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        connection = DriverManager.getConnection("jdbc:mysql://localhost:3307/crudapi", "root", "123456");
        connection.setAutoCommit(false);
        repo = new PeopleRepository(connection);
    }
    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void canSaveOnePerson(){
        Person john = new Person(
                "John", "Smith", ZonedDateTime.of(1960, 11, 15 , 15,15,10,0, ZoneId.of("+0"))
        );
        Person savedPerson = repo.save(john);
        assertThat(savedPerson.getId()).isGreaterThan(0);
    }
    @Test
    public void canSaveTwoPeople(){
       Person john = new Person(
               "John", "Smith", ZonedDateTime.of(1980, 11, 15 , 15,15,0,0, ZoneId.of("-6"))
       );
       Person bobby = new Person(
               "Bobby", "Horse", ZonedDateTime.of(1985, 11, 25 , 15,15,12,0, ZoneId.of("-8"))
       );
       Person savedPerson1 = repo.save(john);
       Person savedPerson2 = repo.save(bobby);
       assertThat(savedPerson1.getId()).isNotEqualTo(savedPerson2.getId());
   }

    @Test
    public void canSavePersonWithHomeAddress() throws SQLException {
        Person john = new Person("JohnZZZZZ", "Smith", ZonedDateTime.of(1980, 11, 15 , 15,15,0,0, ZoneId.of("-6")));
        Address address = new Address(null, "123 Beale St.", "Apt. 1a", "Wala Wala", "WA", "90210", "Fulton County", Region.WEST, "United States");
        john.setHomeAddress(address);

       Person savedPerson = repo.save(john);
       assertThat(savedPerson.getHomeAddress().get().id()).isGreaterThan(0);
   }
    @Test
    public void canSavePersonWithBizAddress() throws SQLException {
        Person john = new Person("JohnZZZZZ", "Smith", ZonedDateTime.of(1980, 11, 15 , 15,15,0,0, ZoneId.of("-6")));
        Address address = new Address(null, "123 Beale St.", "Apt. 1a", "Wala Wala", "WA", "90210", "Fulton County", Region.WEST, "United States");
        john.setBusinessAddress(address);

       Person savedPerson = repo.save(john);
       assertThat(savedPerson.getBusinessAddress().get().id()).isGreaterThan(0);
   }
    @Test
    public void canSavePersonWithChildren() throws SQLException {
        Person john = new Person("JohnZZZZZ", "Smith", ZonedDateTime.of(1980, 11, 15 , 15,15,0,0, ZoneId.of("-6")));
        john.addChild(new Person("Johnny", "Smith", ZonedDateTime.of(2010, 11, 15 , 15,15,0,0, ZoneId.of("-6"))));
        john.addChild(new Person("Sarah", "Smith", ZonedDateTime.of(2012, 11, 15 , 15,15,0,0, ZoneId.of("-6"))));
        john.addChild(new Person("Jenny", "Smith", ZonedDateTime.of(2014, 11, 15 , 15,15,0,0, ZoneId.of("-6"))));

        Person savedPerson = repo.save(john);
        savedPerson.getChildren().stream()
            .map(Person::getId)
            .forEach(id -> assertThat(id).isGreaterThan(0));
    }
    @Test
    public void canSavePersonWithSpouse() throws SQLException {
       Person john = new Person("JohnZZZZZ", "Smith", ZonedDateTime.of(1980, 11, 15 , 15,15,0,0, ZoneId.of("-6")));
       Person joan = new Person("JoannZZZZZ", "Smith", ZonedDateTime.of(1980, 11, 15 , 15,15,0,0, ZoneId.of("-6")));

       Person savedJohn = repo.save(john);
       Person savedJoan = repo.save(joan);
       john.setSpouse(joan);
       assertThat(john.getSpouse().get().getId()).isGreaterThan(0);
   }

    @Test
    public void canFindPersonById(){
       Person savedPerson = repo.save(new Person(2L,"test", "jackson", ZonedDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS)));
       Person foundPerson = repo.findById(savedPerson.getId()).get();
       assertThat(foundPerson).isEqualTo(savedPerson);
   }
    @Test
    public void canFindPersonByIdWithHomeAddress() {
        Person john = new Person("JohnZZZZZ", "Smith", ZonedDateTime.of(1980, 11, 15 , 15,15,0,0, ZoneId.of("-6")));
        Address address = new Address(null, "123 Beale St.", "Apt. 1a", "Wala Wala", "WA", "90210", "Fulton County", Region.WEST, "United States");
        john.setHomeAddress(address);

        Person savedPerson = repo.save(john);
        Person foundPerson = repo.findById(savedPerson.getId()).get();

        assertThat(foundPerson.getHomeAddress().get().state()).isEqualTo("WA");
    }
    @Test
    public void canFindPersonByIdWithSpouse() {
        Person john = new Person("JohnZZZZZ", "Smith", ZonedDateTime.of(1980, 11, 15 , 15,15,0,0, ZoneId.of("-6")));
        Person joan = new Person("JoannZZZZZ", "Smith", ZonedDateTime.of(1980, 11, 15 , 15,15,0,0, ZoneId.of("-6")));
        john.setSpouse(joan);

        Person savedJohn = repo.save(john);
//        Person savedJoan = repo.save(joan);

        Person foundJohn = repo.findById(savedJohn.getId()).get();

        assertThat(foundJohn.getSpouse().get().getId()).isEqualTo(joan.getId());
    }
    @Test
    public void canFindPersonByIdWithBusinessAddress() {
        Person john = new Person("JohnZZZZZ", "Smith", ZonedDateTime.of(1980, 11, 15 , 15,15,0,0, ZoneId.of("-6")));
        Address address = new Address(null, "123 Beale St.", "Apt. 1a", "Wala Wala", "WA", "90210", "Fulton County", Region.WEST, "United States");
        john.setBusinessAddress(address);

        Person savedPerson = repo.save(john);
        Person foundPerson = repo.findById(savedPerson.getId()).get();

        assertThat(foundPerson.getBusinessAddress().get().state()).isEqualTo("WA");
    }
    @Test
    public void canFindPersonByIdWithChildren(){
        Person john = new Person("JohnZZZZZ", "Smith", ZonedDateTime.of(1980, 11, 15 , 15,15,0,0, ZoneId.of("-6")));
        john.addChild(new Person("Johnny", "Smith", ZonedDateTime.of(2010, 11, 15 , 15,15,0,0, ZoneId.of("-6"))));
        john.addChild(new Person("Sarah", "Smith", ZonedDateTime.of(2012, 11, 15 , 15,15,0,0, ZoneId.of("-6"))));
        john.addChild(new Person("Jenny", "Smith", ZonedDateTime.of(2014, 11, 15 , 15,15,0,0, ZoneId.of("-6"))));

        Person savedPerson = repo.save(john);
        Person foundPerson = repo.findById(savedPerson.getId()).get();
        assertThat(foundPerson.getChildren().stream().map(Person::getFirstName).collect(toSet())).contains("Johnny", "Sarah", "Jenny");
    }

    @Test
    public void testPersonIdNotFound(){
        Optional<Person> foundId = repo.findById(-1L);
        assertThat(foundId).isEmpty();
    }

    @Test
//    @Disabled
    public void testNumberOfPeople(){
        repo.save(new Person("John1", "Smith", ZonedDateTime.of(1980, 11, 15 , 15,15,0,0, ZoneId.of("-6"))));
        repo.save(new Person("John2", "Smith", ZonedDateTime.of(1980, 11, 15 , 15,15,0,0, ZoneId.of("-6"))));
        repo.save(new Person("John3", "Smith", ZonedDateTime.of(1980, 11, 15 , 15,15,0,0, ZoneId.of("-6"))));
        repo.save(new Person("John4", "Smith", ZonedDateTime.of(1980, 11, 15 , 15,15,0,0, ZoneId.of("-6"))));
        repo.save(new Person("John5", "Smith", ZonedDateTime.of(1980, 11, 15 , 15,15,0,0, ZoneId.of("-6"))));
        repo.save(new Person("John6", "Smith", ZonedDateTime.of(1980, 11, 15 , 15,15,0,0, ZoneId.of("-6"))));
        repo.save(new Person("John7", "Smith", ZonedDateTime.of(1980, 11, 15 , 15,15,0,0, ZoneId.of("-6"))));
        repo.save(new Person("John8", "Smith", ZonedDateTime.of(1980, 11, 15 , 15,15,0,0, ZoneId.of("-6"))));
        repo.save(new Person("John9", "Smith", ZonedDateTime.of(1980, 11, 15 , 15,15,0,0, ZoneId.of("-6"))));
        repo.save(new Person("John10", "Smith", ZonedDateTime.of(1980, 11, 15 , 15,15,0,0, ZoneId.of("-6"))));

        List<Person> people = repo.findAll();
        assertThat(people.size()).isGreaterThanOrEqualTo(10);

        }

    @Test
    public void canGetCount(){
        long startCount = repo.count();
        repo.save(new Person("John1", "Smith", ZonedDateTime.of(1980, 11, 15 , 15,15,0,0, ZoneId.of("-6"))));
        repo.save(new Person("John2", "Smith", ZonedDateTime.of(1980, 11, 15 , 15,15,0,0, ZoneId.of("-6"))));
        long endCount = repo.count();

        assertThat(endCount).isEqualTo(startCount + 2);
    }

    @Test
    public void canDelete(){
        Person savedPerson = repo.save(new Person("test", "jackson", ZonedDateTime.now().withZoneSameInstant(ZoneId.of("+0"))));
        long startCount = repo.count();
        repo.delete(savedPerson);
        long endCount = repo.count();
        assertThat(endCount).isEqualTo(startCount - 1);
    }

    @Test
    public void canDeleteMultiplePeople(){
        Person p1 = repo.save(new Person("John1", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        Person p2 = repo.save(new Person("John2", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));
        long startCount = repo.count();
        repo.delete(p1, p2);
        long endCount = repo.count();
        assertThat(endCount).isEqualTo(startCount - 2);
    }

    @Test
    public void canUpdate(){
        Person savedPerson = repo.save(new Person("John1", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))));

        Person p1 = repo.findById(savedPerson.getId()).get();

        savedPerson.setSalary(new BigDecimal("73000.34"));
        repo.update(savedPerson);

        Person p2 = repo.findById(savedPerson.getId()).get();

        assertThat(p1.getSalary()).isNotEqualTo(p2.getSalary());
    }

    @Test
    @Disabled
    public void loadData() throws IOException, SQLException {

        Files.lines(Path.of("E:/programowanie/java udemy/Employees/Hr5m.csv"))
                .skip(1)
                .limit(100000)
                .map(s -> s.split(","))
                .map(arr -> {
                    LocalDate dob = LocalDate.parse(arr[10], DateTimeFormatter.ofPattern("M/d/yyyy"));
                    LocalTime tob = LocalTime.parse(arr[11], DateTimeFormatter.ofPattern("hh:mm:ss a"));
                    LocalDateTime dtob = LocalDateTime.of(dob, tob);
                    ZonedDateTime zdtob = ZonedDateTime.of(dtob, ZoneId.of("+0"));
                    Person person = new Person(arr[2], arr[4], zdtob);
                    person.setSalary(new BigDecimal(arr[25]));
                    person.setEmail(arr[6]);
                    return person;
                })
                .forEach(repo::save);
        connection.commit();
    }

    @Test
    public void experiment(){
        LocalDateTime ldt = LocalDateTime.of(1960, 12, 3, 4, 34, 54);
        ZonedDateTime zdt = ZonedDateTime.of(ldt, ZoneId.of("+0"));
        Timestamp tsmt = Timestamp.valueOf(zdt.withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime());
        System.out.println(tsmt);
        System.out.println(zdt);
    }

}
