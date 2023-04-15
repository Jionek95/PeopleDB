package com.jionek.peopledb.model;

public record Address(
        Long id,
        String streetAddress, String address2, String city, String state, String postcode,
        String country, String county, Region region) {
}
