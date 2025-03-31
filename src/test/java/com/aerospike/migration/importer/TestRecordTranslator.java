package com.aerospike.migration.importer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.migration.importer.TranslateSpec.Type;


public class TestRecordTranslator {

    @Test
    void testTranslator() {
        List<MappingSpec> specList = new ArrayList<>();
        
        /*
         * - key: customer:(\d+):addresses:[\d+]:(\w+)
         *   namespace: test
         *   set: Customer
         *   id: $1
         *   path: $.addr.$2
         */
        MappingSpecs specs = new MappingSpecs();

        MappingSpec mappingSpec = new MappingSpec();
        mappingSpec.setNamespace("test");
        mappingSpec.setSet("translator");
        mappingSpec.setKey("customer:(\\d+):addresses:(\\d+):(\\w+)");
        mappingSpec.setId("$1");
        mappingSpec.setPath("$.addr.$2.$3");
        specList.add(mappingSpec);
        
        mappingSpec = new MappingSpec();
        mappingSpec.setNamespace("test");
        mappingSpec.setSet("translator");
        mappingSpec.setKey("customer:(\\d+)");
        mappingSpec.setId("$1");
        mappingSpec.getTranslate().add(new TranslateSpec("**.age", null, Type.INTEGER));
        mappingSpec.getTranslate().add(new TranslateSpec("$.firstName", "fname", null));
        mappingSpec.getTranslate().add(new TranslateSpec("**.heightInCm", "hInCm", Type.INTEGER));
        mappingSpec.getTranslate().add(new TranslateSpec("**.weight", null, Type.DOUBLE));
        specList.add(mappingSpec);
        
        mappingSpec = new MappingSpec();
        mappingSpec.setNamespace("test");
        mappingSpec.setSet("translator");
        mappingSpec.setKey("customer:(\\d+):children:(\\d+):(\\w+)");
        mappingSpec.setId("$1");
        mappingSpec.setPath("$.children[$2].$3");
        specList.add(mappingSpec);
        
        mappingSpec = new MappingSpec();
        mappingSpec.setNamespace("test");
        mappingSpec.setSet("translator");
        mappingSpec.setKey("customer:(\\d+):cars:(\\d+)");
        mappingSpec.setId("$1");
        mappingSpec.setPath("$.cars[$2]");
        mappingSpec.getTranslate().add(new TranslateSpec("**.age", null, Type.INTEGER));
        specList.add(mappingSpec);
        specs.setMappings(specList);

        // Add 2 fields one by one
        String key = "customer:1234:addresses:12:line1";
        RecordTranslator translator = specs.getTranslatorFromString(key, true);
        List<Operation> value = translator.getOperationsFor(null, "123 Main St");

        key = "customer:1234:addresses:12:suburb";
        translator = specs.getTranslatorFromString(key, true);
        value.addAll( translator.getOperationsFor(null, "Denver") );

        // Add a couple of bulk bins
        translator = specs.getTranslatorFromString("customer:1234", true);
        value.addAll( translator.getOperationsFor("age", "37") );
        value.addAll( translator.getOperationsFor("firstName", "Tim") );
        
        // Add a KV map of bins
        translator = specs.getTranslatorFromString("customer:1234", true);
        value.addAll( translator.getOperationsFor(Map.of("heightInCm", "203", "weight", "105.25", "language", "English")));

        // Add in a child
        key = "customer:1234:children:0:firstName";
        translator = specs.getTranslatorFromString(key, true);
        List<Operation> vals = translator.getOperationsFor(null, "Jess");
        value.addAll(vals);
        
        // Add in 2 cars
        key = "customer:1234:cars:0";
        translator = specs.getTranslatorFromString(key, true);
        System.out.println("Ops for make:Toyota");
        value.addAll( translator.getOperationsFor("make", "Toyota") );
        System.out.println("Ops for model:Corolla");
        value.addAll( translator.getOperationsFor("model", "Corolla") );
        System.out.println("Ops for age:28");
        value.addAll( translator.getOperationsFor("age", "28") );
        
        key = "customer:1234:cars:1";
        translator = specs.getTranslatorFromString(key, true);
        System.out.println("Ops for {make:Mitsubishi, model:Lancer, age:17}");
        value.addAll( translator.getOperationsFor(Map.of("make", "Mitusbishi", "model", "Lancer", "age", "17")));
        
        try (IAerospikeClient client = new AerospikeClient("localhost", 3100)) {
            Key asKey = translator.getKey();
            client.delete(null, asKey);
    
            client.operate(null, asKey, value.toArray(new Operation[0]));
            
            Record rec = client.get(null, asKey);
            assertNotNull(rec);
            Map<String, ?> map = (Map<String, ?>) rec.getMap("addr");
            assertNotNull(map);
            map = (Map<String, ?>) map.get("12");
            assertNotNull(map);
            assertEquals(map.get("line1"), "123 Main St");
            assertEquals(map.get("suburb"), "Denver");
            
            assertEquals(37, rec.getInt("age"));
            assertEquals(203, rec.getInt("hInCm"));
            assertEquals("English", rec.getString("language"));
            assertEquals("Tim", rec.getString("fname"));
            assertEquals(105.25, rec.getDouble("weight"), 0.00001);
        }
    }
}
