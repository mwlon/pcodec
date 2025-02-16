package com.pco;

public class InvalidTypeException extends IllegalArgumentException {
    public InvalidTypeException(String expectedType) {
        super("Invalid type access: expected " + expectedType);
    }
}