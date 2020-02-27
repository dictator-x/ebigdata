package com.ebigdata.hadoop.hdfs;

import java.util.Properties;

public class ParamsUtils {
    private static Properties properties = new Properties();
    static {
        try {
            properties.load(ParamsUtils.class.getClassLoader().getResourceAsStream("wc.properties"));
        } catch ( Exception e ) {
            System.out.println(e);
        }
    }

    public static Properties getProperties() throws Exception {
        return properties;
    }
}
