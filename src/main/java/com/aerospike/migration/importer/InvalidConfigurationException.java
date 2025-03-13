package com.aerospike.migration.importer;

public class InvalidConfigurationException extends RuntimeException {
    private static final long serialVersionUID = -7523176812896563113L;

    public InvalidConfigurationException(String message, Object ... args) {
        super(String.format(message, args));
    }
}
