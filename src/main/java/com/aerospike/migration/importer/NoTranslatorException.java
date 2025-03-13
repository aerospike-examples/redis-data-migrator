package com.aerospike.migration.importer;

public class NoTranslatorException extends RuntimeException {
    private static final long serialVersionUID = -6292459996140596781L;

    public NoTranslatorException(String message, Object ... args) {
        super(String.format(message, args));
    }
}
