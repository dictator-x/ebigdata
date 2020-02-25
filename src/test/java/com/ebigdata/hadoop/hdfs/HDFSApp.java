package com.ebigdata.hadoop.hdfs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;

import java.net.URI;

import org.junit.Before;
import org.junit.After;
import org.junit.Test;

public class HDFSApp {

    public static final String HDFS_PATH = "hdfs://hadoop000:8020";
    FileSystem fileSystem = null;
    Configuration configuration = null;

    @Before
    public void setUp() throws Exception {
        System.out.println("-----------setUp---------------");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "hadoop");
    }

    @After
    public void tearDown() {
        configuration = null;
        fileSystem = null;
        System.out.println("------------tearDown-----------");
    }

    @Test
    public void mkdir() throws Exception {
        Path path = new Path("/hdfsapi/test");
        boolean result = fileSystem.mkdirs(path);
        System.out.println(result);
    }

    @Test
    public void text() throws Exception {
        Path path = new Path("/README.txt");
        FSDataInputStream in = fileSystem.open(path);
        IOUtils.copyBytes(in, System.out, 1024);
    }

    public static void main(String[] args) throws Exception {

    }

}
