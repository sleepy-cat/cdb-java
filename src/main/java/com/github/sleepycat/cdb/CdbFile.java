package com.github.sleepycat.cdb;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class CdbFile {

    private RandomAccessFile raf;

    public CdbFile(String fileName, CdbFileMode mode) throws FileNotFoundException {
        raf = new RandomAccessFile(fileName, mode == CdbFileMode.Write ? "rw" : "r");
    }

    public void close() throws IOException {
        raf.close();
    }

    public Datum readDatum() throws IOException {
        int keyLen = readInt();
        int valueLen = readInt();
        return new Datum(readBytes(keyLen), readBytes(valueLen));
    }

    public int writeDatum(Datum value) throws IOException {
        int result = 0;
        result += writeInt(value.getKey().length);
        result += writeInt(value.getValue().length);
        result += writeBytes(value.getKey());
        result += writeBytes(value.getValue());
        return result;
    }

    public Slot readSlot() throws IOException {
        return new Slot(readInt(), readInt());
    }

    public int writeSlot(Slot value) throws IOException {
        int result = 0;
        result += writeInt(value.getHash());
        result += writeInt(value.getDatumOffset());
        return result;
    }

    public HeaderEntry readHeaderEntry() throws IOException {
        return new HeaderEntry(readInt(), readInt());
    }

    public int writeHeaderEntry(HeaderEntry value) throws IOException {
        int result = 0;
        result += writeInt(value.getSlotsOffset());
        result += writeInt(value.getSlotsCount());
        return result;
    }

    public void seek(long pos) throws IOException {
        raf.seek(pos);
    }

    private int readInt() throws IOException {
        byte[] buffer = new byte[4];
        raf.readFully(buffer);
        return ((buffer[0] & 0xFF) << 24) +
                ((buffer[1] & 0xFF) << 16) +
                ((buffer[2] & 0xFF) << 8) +
                (buffer[3] & 0xFF);
    }

    private int writeInt(int value) throws IOException {
        byte[] buffer = new byte[4];
        buffer[0] = (byte) (value >> 24);
        buffer[1] = (byte) (value >> 16);
        buffer[2] = (byte) (value >> 8);
        buffer[3] = (byte) value;
        return writeBytes(buffer);
    }

    private byte[] readBytes(int len) throws IOException {
        byte[] result = new byte[len];
        raf.readFully(result);
        return result;
    }

    private int writeBytes(byte[] b) throws IOException {
        raf.write(b);
        return b.length;
    }
}