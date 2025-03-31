package com.aerospike.migration.importer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

class TestPathParser {

    @Test
    void test() {
        String source = "$.cust[2].details.addr[3].line";
        PathParser pathParser = new PathParser(source, false);
        List<Object> parts = pathParser.parsePath();
        System.out.println(parts);
        assertTrue(parts != null);
        assertEquals(parts.size(), 6);
        assertEquals(parts.get(0), "cust");
        assertEquals(parts.get(1), 2L);
        assertEquals(parts.get(2), "details");
        assertEquals(parts.get(3), "addr");
        assertEquals(parts.get(4), 3L);
        assertEquals(parts.get(5), "line");
    }
    
    @Test
    void testMissingNumber() {
        String source = "$.cust[].details.addr[3].line";
        PathParser pathParser = new PathParser(source, false);
        assertThrows(InvalidConfigurationException.class, () -> {
            pathParser.parsePath();
        });
    }

    @Test
    void testSpaceInsteadOfNumber() {
        String source = "$.cust[ ].details.addr[3].line";
        PathParser pathParser = new PathParser(source, false);
        assertThrows(InvalidConfigurationException.class, () -> {
            pathParser.parsePath();
        });
    }
    
    @Test
    void testSingleGlob() {
        String source = "*.test";
        PathParser pathParser = new PathParser(source, true);
        List<Object> parts = pathParser.parsePath();
        assertEquals(2, parts.size());
    }
}
