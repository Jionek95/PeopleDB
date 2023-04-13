package com.jionek.peopledb.annotation;

import com.jionek.peopledb.model.CrudOperation;

import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
@Repeatable(MultiSql.class)
public @interface SQL {
    String value();
    CrudOperation operationType();
}
