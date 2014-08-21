package com.github.sleepycat.cdb;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CdbFileTest {

    private static final String FILE_NAME = "test-rw";

    CdbFile file;

    @BeforeMethod
    public void setUp() throws Exception {
        file = new CdbFile(FILE_NAME, CdbFileMode.Write);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        Files.delete(Paths.get(FILE_NAME));
    }

    @Test
    public void testReadWriteDatum() throws Exception {
        Datum datum = new Datum(newByteArray(1024), newByteArray(2048));
        file.writeDatum(datum);
        file.seek(0);
        Datum readedDatum = file.readDatum();
        Assert.assertEquals(readedDatum, datum);

    }

    @Test
    public void testReadWriteSlot() throws Exception {
        Slot slot = new Slot(0xAACCBBDD, 0xFFEECCDD);
        file.writeSlot(slot);
        file.seek(0);
        Slot readedSlot = file.readSlot();
        Assert.assertEquals(readedSlot, slot);
    }

    @Test
    public void testReadWriteHeaderEntry() throws Exception {
        HeaderEntry headerEntry = new HeaderEntry(0xAACCBBDD, 0xFFEECCDD);
        file.writeHeaderEntry(headerEntry);
        file.seek(0);
        HeaderEntry readerHeaderEntry = file.readHeaderEntry();
        Assert.assertEquals(readerHeaderEntry, headerEntry);
    }

    private byte[] newByteArray(int len) {
        byte[] result = new byte[len];
        byte b = 0;
        for (int i = 0; i < len; i++) {
            result[i] = b++;
        }
        return result;
    }
}
