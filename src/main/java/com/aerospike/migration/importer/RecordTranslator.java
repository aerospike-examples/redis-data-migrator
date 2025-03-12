package com.aerospike.migration.importer;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
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
    
    public Bin getBin(String key, String value) {
        BinSpec spec = this.mappingSpec.getSpecForBin(key);
        if (value == null) {
            throw new InvalidConfigurationException("Key %s: Value cannot be null for '%s'", redisKey, key);
        }
        if (key == null) {
            if (spec == null || spec.getBinName() == null) {
                throw new InvalidConfigurationException("Key: %s - no bin name has been provided", redisKey);
            }
        }
        Bin bin = null;
        if (spec == null) {
            bin = new Bin(key, value);
        }
        else {
            try {
                String name = spec.getBinName() != null ? matcher.replaceAll(spec.getBinName()) : redisKey;
                switch (spec.getType()) {
                case BOOLEAN:
                    bin = new Bin(name, value.equals("1") || value.equalsIgnoreCase("true") || value.equalsIgnoreCase("y") || value.equalsIgnoreCase("yes"));
                    break;
                case INTEGER:
                    bin = new Bin(name, Long.parseLong(value));
                    break;
                case DOUBLE:
                    bin = new Bin(name, Double.parseDouble(value));
                    break;
                case BYTES:
                    bin = new Bin(name, Utils.hexStringToByteArray(value));
                    break;
                case LIST:
                    bin = new Bin(name, (List<?>)listReader.readValue(value));
                    break;
                case MAP:
                    bin = new Bin(name, (Map<?, ?>)mapReader.readValue(value));
                    break;
                default:
                    bin = new Bin(name, value);
                }
            }
            catch (JsonProcessingException jpe) {
                throw new RuntimeException(jpe.getMessage(), jpe);
            }
        }
        if (bin.name == null) {
            throw new InvalidConfigurationException("Key %s: bin name could not be dervied for value %s", redisKey, value);
        }
        return bin;
    }
}
