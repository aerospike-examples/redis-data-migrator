package com.aerospike.migration.importer;

public class Utils {
    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        if (len%2 == 1) {
            // This must be an even length string. Prepend a 0
            s = "0" + s;
            len++;
        }
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                                 + Character.digit(s.charAt(i+1), 16));
        }
        return data;
    }
    
    
}
