package com.aerospike.migration.importer;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.migration.importer.BinSpec.Type;
import com.aerospike.migration.importer.MappingSpec.KeyType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

public class RecordTranslator {
    private final static ObjectReader listReader = new ObjectMapper().readerFor(List.class);
    private final static ObjectReader mapReader = new ObjectMapper().readerFor(Map.class);
    private final MappingSpec mappingSpec;
    private final Matcher matcher;
    private final String redisKey;
    
    public RecordTranslator(MappingSpec mappingSpec, Matcher matcher, String redisKey) {
        this.mappingSpec = mappingSpec;
        this.matcher = matcher;
        this.redisKey = redisKey;
    }
    
    public Key getKey() {
        String namespace = matcher.replaceAll(mappingSpec.getNamespace());
        String set = matcher.replaceAll(mappingSpec.getSet());
        String id = matcher.replaceAll(mappingSpec.getId());
        if (mappingSpec.getType() == KeyType.INTEGER) {
            return new Key(namespace, set, Long.parseLong(id));
        }
        else if (mappingSpec.getType() == KeyType.BLOB) {
            return new Key(namespace, set, Utils.hexStringToByteArray(id));
        }
        else {
            return new Key(namespace, set, id);
        }
    }
    
    public Type getBinType(String key) {
        BinSpec spec = this.mappingSpec.getSpecForBin(key);
        if (spec != null && spec.getType() != null) {
            return spec.getType();
        }
        return Type.STRING;
    }
    
    public Object convertToType(String value, Type type) {
        try {
            switch(type) {
            case BOOLEAN:
                return value.equals("1") || value.equalsIgnoreCase("true") || value.equalsIgnoreCase("y") || value.equalsIgnoreCase("yes");
            case INTEGER:
                return Long.parseLong(value);
            case DOUBLE:
                return Double.parseDouble(value);
            case BYTES:
                return Utils.hexStringToByteArray(value);
            case LIST:
                return (List<?>)listReader.readValue(value);
            case MAP:
                return (Map<?, ?>)mapReader.readValue(value);
            default:
                return value;
            }
        }
        catch (JsonProcessingException jpe) {
            throw new RuntimeException(jpe.getMessage(), jpe);
        }
    }
    
    public String getBinName(String key) {
        BinSpec spec = this.mappingSpec.getSpecForBin(key);
        if (key == null && (spec == null || spec.getBinName() == null)) {
            throw new InvalidConfigurationException("Key: %s - no bin name has been provided", redisKey);
        }
        if (spec == null) {
            return key;
        }
        else {
            return spec.getBinName() != null ? matcher.replaceAll(spec.getBinName()) : key;
        }
    }

    public Bin getBin(String key, String value) {
        if (value == null) {
            throw new InvalidConfigurationException("Key %s: Value cannot be null for '%s'", redisKey, key);
        }
        String binName = getBinName(key);
        BinSpec spec = this.mappingSpec.getSpecForBin(key);
        Bin bin = null;
        if (spec == null) {
            bin = new Bin(binName, value);
        }
        else {
            bin = new Bin(binName, Value.get(convertToType(value, spec.getType())));
        }
        if (bin.name == null) {
            throw new InvalidConfigurationException("Key %s: bin name could not be dervied for value %s", redisKey, value);
        }
        return bin;
    }
}
