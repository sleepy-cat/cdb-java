package com.github.sleepycat.cdb.exception;

public class CdbError extends Exception {

    public CdbError(String message) {
        super(message);
    }

    public CdbError(Throwable cause) {
        super(cause);
    }
}
