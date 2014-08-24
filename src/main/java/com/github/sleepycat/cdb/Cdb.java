package com.github.sleepycat.cdb;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

public class Cdb implements AutoCloseable {

    private final CdbFile file;
    private final Header header;

    public Cdb(String fileName) throws IOException {
        file = new CdbFile(fileName, CdbFileMode.Read);
        if (file.size() < Header.BYTES_SIZE) {
            throw new IOException("Unexpected end of CDB file. At least " + Header.BYTES_SIZE + " bytes expected.");
        }
        header = file.readHeader();
    }

    @Override
    public void close() throws Exception {
        file.close();
    }

    public Optional<byte[]> get(byte[] key) throws IOException {
        int keyHash = CdbHash.calculate(key);
        HeaderEntry headerEntry = header.get(CdbHash.modulo(keyHash, Header.ENTRY_COUNT));
        if (headerEntry.getSlotsCount() == 0) {
            return Optional.empty();
        }

        int start = (keyHash >>> 8) % headerEntry.getSlotsCount();
        for (int i = 0; i < headerEntry.getSlotsCount(); i++) {
            file.seek(headerEntry.getSlotsOffset() + ((start + i) % headerEntry.getSlotsCount()) * Slot.BYTES_SIZE);
            Slot slot = file.readSlot();
            if (Slot.EMPTY_SLOT.equals(slot)) {
                return Optional.empty();
            }
            if (keyHash == slot.getHash()) {
                file.seek(slot.getDatumOffset());
                Datum datum = file.readDatum();
                if (Arrays.equals(key, datum.getKey())) {
                    return Optional.of(datum.getValue());
                }
            }
        }

        return Optional.empty();
    }
}
