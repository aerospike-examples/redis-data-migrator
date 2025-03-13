package com.aerospike.migration.importer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.Data;

@Data
public class MappingSpec {
    public enum KeyType {STRING, BLOB, INTEGER};
    private final String NULL_STR = "__null__";
    private String key;
    private String namespace;
    private String set;
    private String id;
    private KeyType type = KeyType.STRING;
    private final List<BinSpec> bins = new ArrayList<>();
    private final Map<String, BinSpec> binMap = new HashMap<>();
    private Pattern pattern;
    
    public void setKey(String key) {
        this.key = key;
        this.pattern = Pattern.compile(key);
    }
    
    public Matcher matches(String key) {
        Matcher matcher = this.pattern.matcher(key);
        if (matcher.matches()) {
            return matcher;
        }
        return null;
    }
    
    public void validate() {
        for (BinSpec thisBinSpec : bins) {
            String binName = thisBinSpec.getName();
            if (binName == null) {
                binName = NULL_STR;
            }
            BinSpec existingSpec = binMap.get(binName);
            if (existingSpec != null) {
                throw new InvalidConfigurationException("Key %s: Bin spec for bin %s is duplicated", key, binName);
            }
            binMap.put(binName, thisBinSpec);
        }
    }
    
    public BinSpec getSpecForBin(String name) {
        if (name == null) {
            name = NULL_STR;
        }
        return this.binMap.get(name);
    }
}
