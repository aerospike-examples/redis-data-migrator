package com.aerospike.migration.importer;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.Data;

@Data
public class MappingSpec {
    public enum KeyType {STRING, BLOB, INTEGER};
    private String key;
    private String namespace;
    private String set;
    private String id;
    private String path;
    private Boolean sendKey; 
    private KeyType type = KeyType.STRING;
    private final List<TranslateSpec> translate = new ArrayList<>();
    private Pattern pattern;
    
    public void setKey(String key) {
        this.key = key;
        this.pattern = Pattern.compile(key);
    }
    
    public Boolean sendKey() {
        return this.sendKey;
    }
    
    public Matcher matches(String key) {
        Matcher matcher = this.pattern.matcher(key);
        if (matcher.matches()) {
            return matcher;
        }
        return null;
    }
    
    public TranslateSpec findMatchingSpec(Deque<Object> currentPath) {
        for (int i = 0; i < translate.size(); i++) {
            if (translate.get(i).matches(currentPath)) {
                return translate.get(i);
            }
        }
        return null;
    }
}
