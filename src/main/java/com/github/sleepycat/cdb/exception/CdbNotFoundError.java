package com.github.sleepycat.cdb.exception;

public class CdbNotFoundError extends CdbError {

    private byte[] key;

    public CdbNotFoundError(byte[] key) {
        super("Key not found");
        this.key = key;
    }

    @SuppressWarnings("unused")
    public byte[] getKey() {
        return key;
    }
}
