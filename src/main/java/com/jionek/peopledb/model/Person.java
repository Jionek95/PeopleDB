package com.jionek.peopledb.model;

import java.time.ZonedDateTime;
import java.util.Objects;

public class Person {
    private Long id;
    private String firstName;
    private String lastName;
    private ZonedDateTime dob;
    public Person (String firstName, String lastName, ZonedDateTime dob) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.dob = dob;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public ZonedDateTime getDob() {
        return dob;
    }

    public void setDob(ZonedDateTime dob) {
        this.dob = dob;
    }

    @Override
    public String toString() {
        return "Person{" +
                "id=" + id +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", dob=" + dob +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Person person)) return false;
        return Objects.equals(id, person.id) && firstName.equals(person.firstName) && lastName.equals(person.lastName) && dob.equals(person.dob);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, firstName, lastName, dob);
    }
}
