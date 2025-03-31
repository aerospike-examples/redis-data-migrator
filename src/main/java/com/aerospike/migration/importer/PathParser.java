package com.aerospike.migration.importer;

import java.util.ArrayList;
import java.util.List;

public class PathParser {
    private final String path;
    private int index = 0;
    private boolean allowGlobs;
    private final StringBuilder sb = new StringBuilder();
    
    public PathParser(String path, boolean allowGlobs) {
        this.path = path;
        this.allowGlobs = allowGlobs;
    }

    private boolean hasMore() {
        return index < path.length();
    }
    private char current() {
        return path.charAt(index);
    }
    
    private void consumeWhitespace() {
        while (hasMore() && Character.isWhitespace(current())) {
            index++;
        }
    }
    
    private String getGlob() {
        if (!allowGlobs) {
            return null;
        }
        int startIndex = index;
        if (current() == '*') {
            index++;
            if (current() == '*') {
                index++;
            }
            consumeWhitespace();
            if (!(current() == ']' || current() == '.')) {
                throw new InvalidConfigurationException(
                        "Path %s contains glob (* or **) with illegal terminator of %c at index %d. Either '.' or ']' was expected.", 
                        path, current(), index);
            }
        }
        if (startIndex == index) {
            return null;
        }
        return path.substring(startIndex, index);
    }
    
    private void checkAndConsume(char expected) {
        if (!hasMore()) {
            throw new InvalidConfigurationException("Was expecting '%c' in '%s', but end-of-input encountered", expected, path);
        }
        if (current() != expected) {
            throw new InvalidConfigurationException("Was expecting '%c' at location %d in '%s', found '%c'", expected, index+1, path, current());
        }
        index++;
    }
    
    public List<Object> parsePath() {
        // Globs in the path negate the need for the path to start with $.
        if (!(allowGlobs && path.startsWith("*"))) {
            if (path.length() < 2 || path.charAt(index) != '$' || path.charAt(index+1) != '.') {
                throw new InvalidConfigurationException("Path '%s' must start with '$.'", path);
            }
            index += 1;
        }
        List<Object> pathParts = new ArrayList<>();
        String glob;
        while (hasMore()) {
            switch (current()) {
            case '*':
                pathParts.add(getGlob());
                break;
                
            case '[':
                index++;
                consumeWhitespace();
                glob = getGlob();
                if (glob != null) {
                    pathParts.add(glob);
                }
                else {
                    pathParts.add(parseNumber());
                }
                consumeWhitespace();
                checkAndConsume(']');
                break;
                
            case '.':
                index++;
                glob = getGlob();
                if (glob != null) {
                    pathParts.add(glob);
                }
                else {
                    pathParts.add(parsePathPart());
                }
                break;
            }
        }
        return pathParts;
    }
    
    private long parseNumber() {
        int startIndex = index;
        while (Character.isDigit(current()) || Character.isWhitespace(current())) {
            index++;
        }
        if (startIndex == index) {
            throw new InvalidConfigurationException("Path '%s' must contain digit(s) at location %d", path, index+1);
        }
        String data = path.substring(startIndex, index).trim();
        if (data.isEmpty()) {
            throw new InvalidConfigurationException("Path '%s' must contain digit(s) at location %d but contains only whitespace", path, index+1);
        }
        return Long.parseLong(data);
    }
    private String parsePathPart() {
        sb.setLength(0);
        int startIndex = index;
        while (index < path.length()) {
            char ch = current();
            if (ch == '\\') {
                if (++index >= path.length()) {
                    throw new InvalidConfigurationException("Path %s contains a dangling escape at end of string",  path);
                }
                sb.append(path.charAt(index));
            }
            else if (ch == '[' || ch == '.') {
                break;
            }
            else if (Character.isWhitespace(ch)) {
                throw new InvalidConfigurationException("Path %s contains an unescaped whitespace at location %d",  path, index+1);
            }
            else {
                sb.append(ch);
            }
            index++;
        }
        if (sb.length() == 0) {
            throw new InvalidConfigurationException("Expected an idenitifer at location %d in %s but did not receive one", startIndex+1, path);
        }
        return sb.toString();
    }
}
