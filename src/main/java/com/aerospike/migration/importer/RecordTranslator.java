package com.aerospike.migration.importer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.ListWriteFlags;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.migration.importer.MappingSpec.KeyType;
import com.aerospike.migration.importer.TranslateSpec.Type;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

public class RecordTranslator {
    private final static ObjectReader listReader = new ObjectMapper().readerFor(List.class);
    private final static ObjectReader mapReader = new ObjectMapper().readerFor(Map.class);
    private final MappingSpec mappingSpec;
    private final Matcher matcher;
    private final String redisKey;
    private final boolean debug;
    private final static CTX[] CTX_TYPE = new CTX[0]; 
    
    public RecordTranslator(MappingSpec mappingSpec, Matcher matcher, String redisKey, boolean debug) {
        this.mappingSpec = mappingSpec;
        this.matcher = matcher;
        this.redisKey = redisKey;
        this.debug = debug;
    }
    
    public Boolean sendKey() {
        return this.mappingSpec.sendKey();
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
    
    public Object convertToType(String value, Type type) {
        try {
            if (type == null) {
                return value;
            }
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
    
    private void validatePath(List<Object> path) {
        if (path == null) {
            throw new InvalidConfigurationException("path is null");
        }
        else if (path.size() == 0) { 
            throw new InvalidConfigurationException("path parsed to an empty list");
        }
        else if (!(path.get(0) instanceof String)) {
            throw new InvalidConfigurationException("path must start with a bin name");
        }
    }
    /**
     * Create the operations to be able to store this key and value. Consider different use cases:
     * 1. passed a map at the top level. The map becomes a set of bins, one field per bin. Path is /fieldName or mappingPath(key)/fieldName if key is provided.
     * 2. passed a map at a lower level. To do this the path becomes: mappingPath(key)/fieldName
     * 3. passed an individual attribute at the top level. The fieldname will be null, the key will contain the path
     * @param key
     * @param binName
     * @param value
     * @return
     */

    /**
     * Perform a translation of the path, but paths should contain a leading "$." This is
     * an invalid replacement group, so strip it out, translate, then replace
     * @param rawPath
     * @return
     */
    private String getTranslatedPath(String rawPath) {
        if (rawPath.startsWith("$")) {
            return "$" + matcher.replaceAll(rawPath.substring(1));
        }
        else {
            return matcher.replaceAll(rawPath);
        }
    }
    
    private Object applyTranslateSpecToValueAsObject(TranslateSpec spec, String value) {
        String translatedValue = matcher.replaceAll(value);
        if (spec == null) {
            return translatedValue;
        }
        else {
            return convertToType(translatedValue, spec.getType());
        }
    }
    
    private Value applyTranslateSpecToValue(TranslateSpec spec, String value) {
        return Value.get(applyTranslateSpecToValueAsObject(spec, value));
    }
    
    private Object applyTranslateSpecToPathItem(TranslateSpec spec, Object pathItem) {
        if (pathItem instanceof String) {
            String strPathItem = (String)pathItem;
            if (spec != null && spec.getName() != null) {
                return matcher.replaceAll(spec.getName());
            }
            return matcher.replaceAll(strPathItem);
        }
        else {
            return pathItem;
        }
    }
    
    private void putIntoBin(Deque<Object> currentPath, String key, String value, List<Operation> ops) {
        currentPath.push(key);
        TranslateSpec spec = this.mappingSpec.findMatchingSpec(currentPath);
        Value valueToUse = applyTranslateSpecToValue(spec, value);
        String binName = (String)applyTranslateSpecToPathItem(spec, key);
        if (debug) {
            System.out.printf(" - Put '%s' into bin %s\n", valueToUse, binName);
        }
        ops.add(Operation.put(new Bin(binName, valueToUse)));
        currentPath.pop();
    }
    
    private List<Object> translateList(List<String> values, Deque<Object> currentPath) {
        List<Object> newList = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            String thisValue = values.get(i);
            currentPath.push((long)i);
            TranslateSpec listSpec = this.mappingSpec.findMatchingSpec(currentPath);
            newList.add(applyTranslateSpecToValueAsObject(listSpec, thisValue));
            currentPath.pop();
        }
        return newList;
    }
    
    private List<Operation> createNestedOperations(String binName, List<Object> path, Deque<Object> currentPath, List<CTX> ctxs, List<String> ctxStrings) {
        List<Operation> ops = new ArrayList<>();
        for (int i = 1; i < path.size(); i++) {
            Object thisItem = path.get(i);
            currentPath.push(thisItem);
            TranslateSpec spec = this.mappingSpec.findMatchingSpec(currentPath);
            thisItem = applyTranslateSpecToPathItem(spec, thisItem);

            if (thisItem instanceof String) {
                if (debug) {
                    System.out.printf(" - Create map in bin %s with context %s\n", binName, ctxStrings);
                }
                ops.add(MapOperation.create(binName, MapOrder.KEY_ORDERED, ctxs.size() > 0 ? ctxs.toArray(CTX_TYPE) : null));
                if (i < path.size() - 1) {
                    ctxs.add(CTX.mapKey(Value.get(thisItem)));
                    if (debug) {
                        ctxStrings.add(String.format("mapKey(Value.get(\"%s\"))", thisItem));
                    }
                }
            }
            else {
                long index = (long)thisItem;
                if (debug) {
                    System.out.printf(" - Create list in bin %s with context %s\n", binName, ctxStrings);
                }
                ops.add(ListOperation.create(binName, ListOrder.UNORDERED, true, ctxs.size() > 0 ? ctxs.toArray(CTX_TYPE) : null));
                if (i < path.size() - 1) {
                    ctxs.add(CTX.listIndex((int)index));
                    if (debug) {
                        ctxStrings.add(String.format("listIndex(%d)", index));
                    }
                }
            }
        }
        return ops;
    }

    private Operation createFinalOperation(String binName, Object lastOp, Object value, List<CTX> ctxs, List<String> ctxStrings) {
        if (lastOp instanceof String) {
            MapPolicy mapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
            if (debug) {
                System.out.printf(" - MapOperation.put(%s, %s, %s, %s)\n", binName, lastOp, value, ctxStrings);
            }
            return MapOperation.put(mapPolicy, binName, Value.get(lastOp), Value.get(value), ctxs.size() > 0 ? ctxs.toArray(CTX_TYPE) : null);
        }
        else {
            if (debug) {
                System.out.printf(" - ListOperation.set(%s, %s, %s, %s)\n", binName, lastOp, value, ctxStrings);
            }
            ListPolicy listPolicy = new ListPolicy(ListOrder.UNORDERED, ListWriteFlags.DEFAULT);
            return ListOperation.set(listPolicy, binName, (int)(long)lastOp, Value.get(value), ctxs.size() > 0 ? ctxs.toArray(CTX_TYPE) : null);
        }
    }

    public List<Operation> getOperationsFor(List<String> values) {
        if (debug) {
            System.out.printf("Getting operations for list: %s on key '%s'\n"
                    + " - Mapping spec: %s\n",
                    values, this.redisKey, this.mappingSpec);
        }
        Deque<Object> currentPath = new ArrayDeque<>();
        List<Operation> ops = new ArrayList<>();
        
        String pathToUse = this.mappingSpec.getPath();

        if (pathToUse == null) {
            throw new InvalidConfigurationException("List operation on key %s did not contain a path, so I have no idea where to put the data", this.redisKey);
        }

        PathParser parser = new PathParser(getTranslatedPath(pathToUse), false);
        List<Object> path = parser.parsePath();
        validatePath(path);
        currentPath.push(path.get(0));
        
        if (path.size() == 1) {
            TranslateSpec spec = this.mappingSpec.findMatchingSpec(currentPath);
            String binName = (String)applyTranslateSpecToPathItem(spec, path.get(0));
            List<Object> newList = translateList(values, currentPath);
            if (debug) {
                System.out.printf(" - Put '%s' into bin %s\n", newList, binName);
            }
            ops.add(Operation.put(new Bin(binName, newList)));
            currentPath.pop();
            return ops;
        }

        TranslateSpec spec = this.mappingSpec.findMatchingSpec(currentPath);
        String binName = (String)applyTranslateSpecToPathItem(spec, (String)path.get(0));
        
        List<String> ctxStrings = new ArrayList<>();
        List<CTX> ctxs = new ArrayList<>();

        ops.addAll(createNestedOperations(binName, path, currentPath, ctxs, ctxStrings));
        
        Object lastOp = path.get(path.size() - 1);
        List<Object> newList = translateList(values, currentPath);
        ops.add(createFinalOperation(binName, lastOp, newList, ctxs, ctxStrings));

        return ops;
    }

    public List<Operation> getOperationsFor(Map<String, String> namesAndValues) {
        if (debug) {
            System.out.printf("Getting operations for map: %s on key '%s'\n"
                    + " - Mapping spec: %s\n",
                    namesAndValues, this.redisKey, this.mappingSpec);
        }
        Deque<Object> currentPath = new ArrayDeque<>();
        List<Operation> ops = new ArrayList<>();
        
        String pathToUse = this.mappingSpec.getPath();

        if (pathToUse == null) {
            // Need to turn the maps into a sequence of bin
            for (String thisName : namesAndValues.keySet()) {
                putIntoBin(currentPath, thisName, namesAndValues.get(thisName), ops);
            }
            return ops;
        }

        PathParser parser = new PathParser(getTranslatedPath(pathToUse), false);
        List<Object> path = parser.parsePath();
        validatePath(path);
        currentPath.push(path.get(0));

        TranslateSpec spec = this.mappingSpec.findMatchingSpec(currentPath);
        String binName = (String)applyTranslateSpecToPathItem(spec, (String)path.get(0));
        
        List<String> ctxStrings = new ArrayList<>();
        List<CTX> ctxs = new ArrayList<>();

        ops.addAll(createNestedOperations(binName, path, currentPath, ctxs, ctxStrings));
        
        Object lastOp = path.get(path.size() - 1);
        Map<String, Object> newMap = new HashMap<>();
        for (String mapKey : namesAndValues.keySet()) {
            Object mapValue = namesAndValues.get(mapKey);
            currentPath.push(mapKey);
            TranslateSpec mapSpec = this.mappingSpec.findMatchingSpec(currentPath);
            Object valueToUse = (mapValue instanceof String) ? applyTranslateSpecToValueAsObject(mapSpec, (String)mapValue) : mapValue;
            newMap.put((String)applyTranslateSpecToPathItem(mapSpec, mapKey), valueToUse);
        }
        ops.add(createFinalOperation(binName, lastOp, newMap, ctxs, ctxStrings));

        return ops;
    }

    public List<Operation> getOperationsFor(String fieldName, String value) {
        if (debug) {
            System.out.printf("Getting operations for mapping field %s, value '%s' on key '%s'\n"
                    + " - Mapping spec: %s\n",
                    fieldName, value, this.redisKey, this.mappingSpec);
        }
        Deque<Object> currentPath = new ArrayDeque<>();
        if (value == null) {
            throw new InvalidConfigurationException("Key %s: Value cannot be null", redisKey);
        }
        List<Operation> ops = new ArrayList<>();
        
        String pathToUse = this.mappingSpec.getPath();
        if (pathToUse == null) {
            if (fieldName == null) {
                throw new InvalidConfigurationException("cannot map operations without either a mapping path or a field name");
            }
            pathToUse = fieldName.startsWith("$.") ? fieldName : "$." + fieldName;
        }
        else if (fieldName != null) {
            pathToUse += "." + fieldName;
        }

        PathParser parser = new PathParser(getTranslatedPath(pathToUse), false);
        List<Object> path = parser.parsePath();
        validatePath(path);
        if (path.size() == 1) {
            putIntoBin(currentPath, (String)path.get(0), value, ops);
            return ops;
        }

        currentPath.push((String)path.get(0));
        TranslateSpec spec = this.mappingSpec.findMatchingSpec(currentPath);
        String binName = (String)applyTranslateSpecToPathItem(spec, (String)path.get(0));
        
        List<String> ctxStrings = new ArrayList<>();
        List<CTX> ctxs = new ArrayList<>();

        ops.addAll(createNestedOperations(binName, path, currentPath, ctxs, ctxStrings));
        
        Object lastOp = path.get(path.size() - 1);
        Value valueToUse = applyTranslateSpecToValue(spec, value);
        ops.add(createFinalOperation(binName, lastOp, valueToUse, ctxs, ctxStrings));

        return ops;
    }
}
