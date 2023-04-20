package com.jionek.peopledb.exception;

public class DataException extends RuntimeException{
    public DataException(String messsage) {
        super(messsage);
    }

    public DataException(String messsage, Throwable e) {
        super(messsage,e);
    }
}
