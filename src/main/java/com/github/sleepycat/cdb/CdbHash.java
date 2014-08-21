package com.github.sleepycat.cdb;

public class CdbHash {

    // Prohibit instance creation
    private CdbHash() {
    }

    public static int calculate(byte[] bytes) {
        int result = 5381;
        for (byte b : bytes) {
            long l = result;
            result = (int) (((l << 5 + l) ^ b) & 0xFFFFFFFFL);
        }
        return result;
    }
}
