package com.ebigdata.hadoop.hdfs;

public interface Mapper {
    public void map(String line, MapContext context);
}
