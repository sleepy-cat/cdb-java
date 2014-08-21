package com.github.sleepycat.cdb;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class CdbTest {

    private static final String FILE_NAME = "test.cdb";

    @AfterMethod
    public void tearDown() throws Exception {
        Files.delete(Paths.get(FILE_NAME));
    }

    @Test
    public void testGet() throws Exception {
        byte[] key = new byte[]{(byte) 0xFF, (byte) 0xBB, (byte) 0xCC, (byte) 0x1, (byte) 0x2, (byte) 0x3};
        byte[] value = new byte[]{(byte) 0xAA, (byte) 0xBB, (byte) 0xCC, (byte) 0x1, (byte) 0x2, (byte) 0x3};
        try (CdbMaker writer = new CdbMaker(FILE_NAME)) {
            writer.put(key, value);
        }
        try (Cdb cdb = new Cdb(FILE_NAME)) {
            byte[] v = cdb.get(key);
            Assert.assertEquals(v, value);
        }
    }

    @Test
    public void testMassiveGet() throws Exception {
        Map<byte[], byte[]> map = new HashMap<>(100_000);

        try (CdbMaker maker = new CdbMaker(FILE_NAME)) {
            for (int n = 0; n < 100_000; n++) {
                byte[] key = TestUtils.randomBytes(2048);
                byte[] value = TestUtils.randomBytes(4 * 2048);
                map.put(key, value);
                maker.put(key, value);
            }
        }

        try (Cdb cdb = new Cdb(FILE_NAME)) {
            for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
                byte[] v = cdb.get(entry.getKey());
                Assert.assertEquals(v, entry.getValue());
            }
        }
    }
}
