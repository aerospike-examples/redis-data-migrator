package com.aerospike.migration.importer;

import java.util.List;
import java.util.regex.Matcher;

import lombok.Data;

@Data
public class MappingSpecs {
    private List<MappingSpec> mappings;
    
    public RecordTranslator getTranslatorFromString(String redisKey, boolean debug) {
        for (MappingSpec spec : mappings) {
            Matcher matcher = spec.matches(redisKey);
            if (matcher != null) {
                return new RecordTranslator(spec, matcher, redisKey, debug);
            }
        }
        throw new NoTranslatorException("No translator available for key %s, cannot map to Aerospike", redisKey);
    }
    
    public void validate() {
//        for (MappingSpec thisSpec : mappings) {
//            thisSpec.validate();
//        }
    }
}
