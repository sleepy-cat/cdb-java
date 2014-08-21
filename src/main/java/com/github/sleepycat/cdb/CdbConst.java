package com.github.sleepycat.cdb;

public class CdbConst {

    public static final int HEADER_ENTRY_COUNT = 256;
    public static final int HEADER_BYTES_SIZE = HeaderEntry.BYTES_SIZE * CdbConst.HEADER_ENTRY_COUNT;

    // Prohibit instance creation
    private CdbConst() {
    }
}
