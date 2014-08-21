package com.github.sleepycat.cdb;

import java.io.IOException;
import java.util.Arrays;

import com.github.sleepycat.cdb.exception.CdbError;
import com.github.sleepycat.cdb.exception.CdbIOError;
import com.github.sleepycat.cdb.exception.CdbNotFoundError;

public class Cdb implements AutoCloseable {

    private CdbFile file;

    public Cdb(String fileName) throws IOException {
        file = new CdbFile(fileName, CdbFileMode.Read);
    }

    @Override
    public void close() throws Exception {
        file.close();
    }

    public byte[] get(byte[] key) throws CdbError {
        try {
            int keyHash = CdbHash.calculate(key);
            file.seek(Integer.remainderUnsigned(keyHash, CdbConst.HEADER_ENTRY_COUNT) * HeaderEntry.BYTES_SIZE);
            HeaderEntry headerEntry = file.readHeaderEntry();
            if (headerEntry.getSlotsCount() == 0) {
                throw new CdbNotFoundError(key);
            }

            int start = (keyHash >>> 8) % headerEntry.getSlotsCount();
            for (int i = 0; i < headerEntry.getSlotsCount(); i++) {
                file.seek(headerEntry.getSlotsOffset() + ((start + i) % headerEntry.getSlotsCount()) * Slot.BYTES_SIZE);
                Slot slot = file.readSlot();
                if (Slot.EMPTY_SLOT.equals(slot)) {
                    throw new CdbNotFoundError(key);
                }
                if (keyHash == slot.getHash()) {
                    file.seek(slot.getDatumOffset());
                    Datum datum = file.readDatum();
                    if (Arrays.equals(key, datum.getKey())) {
                        return datum.getValue();
                    }
                }
            }
            throw new CdbNotFoundError(key);
        } catch (IOException e) {
            throw new CdbIOError(e);
        }
    }
}
