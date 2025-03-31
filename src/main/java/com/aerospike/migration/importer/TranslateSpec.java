package com.aerospike.migration.importer;

import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class TranslateSpec {
    public enum Type {INTEGER, DOUBLE, STRING, BOOLEAN, BYTES, LIST, MAP }
    private String path;
    private String name;
    private Type type = Type.STRING;
    private List<Object> pathParts;
    
    public TranslateSpec(String path, String name, Type type) {
        super();
        this.path = path;
        this.name = name;
        this.type = type;
    }

    public boolean matches(Deque<Object> matchingPathParts) {
        if (this.pathParts == null) {
            this.pathParts = new PathParser(this.path, true).parsePath();
        }
        
        int index = 0;
        boolean isOnGlob = false;
        for (Iterator<Object> iter = matchingPathParts.descendingIterator(); iter.hasNext();) {
            Object thisPathPart = iter.next();
            if (index >= this.pathParts.size()) {
                return false;
            }
            if ("*".equals(this.pathParts.get(index))) {
                // this always matches
                index++;
                continue;
            }
            else if ("**".equals(this.pathParts.get(index))) {
                isOnGlob = true;
                index++;
                if (index > this.pathParts.size()) {
                    return true;
                }
            }
            if (isOnGlob) {
                if (index >= this.pathParts.size()) {
                    // This has a wildcard at the end, it matches
                    return true;
                }
                else {
                    // Does this part of the path match the next fixed part?
                    if (thisPathPart.equals(this.pathParts.get(index))) {
                        isOnGlob = false;
                        index++;
                    }
                }
            }
            else {
                if (thisPathPart.equals(this.pathParts.get(index))) {
                    index++;
                }
                else {
                    return false;
                }
            }
        }
        if (index < this.pathParts.size()) {
            // This matches only if the remaining items are ** globs
            for (int i = index; i < this.pathParts.size(); i++) {
                if (!"**".equals(this.pathParts.get(index))) {
                    return false;
                }
            }
        }
        return true;
    }


}
