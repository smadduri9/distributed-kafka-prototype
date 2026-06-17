package com.kafkads.storage;

import java.io.IOException;

/** Thrown when a stored record fails its CRC32 check on read. */
public class CorruptRecordException extends IOException {
    public CorruptRecordException(String message) {
        super(message);
    }
}
