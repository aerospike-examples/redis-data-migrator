package com.aerospike.migration.importer;

public class InvalidConfigurationException extends RuntimeException {
    public InvalidConfigurationException(String message, Object ... args) {
        super(String.format(message, args));
    }
}
