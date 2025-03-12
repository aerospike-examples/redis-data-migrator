package com.aerospike.migration.importer;

import lombok.Data;

@Data
public class BinSpec {
    public enum Type {INTEGER, DOUBLE, STRING, BOOLEAN, BYTES, LIST, MAP }
    private String name;
    private String binName;
    private Type type = Type.STRING;
}
