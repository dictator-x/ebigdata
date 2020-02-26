package com.ebigdata.hadoop.hdfs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.net.URI;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.File;

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
        configuration.set("dfs.replication", "1");
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

    @Test
    public void create() throws Exception {
        // Path path = new Path("/hdfsapi/test/a.txt");
        Path path = new Path("/hdfsapi/test/b.txt");
        FSDataOutputStream out = fileSystem.create(path);
        out.writeUTF("hello pack");
        out.flush();
        out.close();
    }

    @Test
    public void rename() throws Exception {
        Path from = new Path("/hdfsapi/test/b.txt");
        Path to = new Path("/hdfsapi/test/c.txt");
        System.out.println(fileSystem.rename(from, to));
    }

    @Test
    public void testReplication() {
        System.out.println(configuration.get("dfs.replication"));
    }

    @Test
    public void copyFromLocal() throws Exception {
        Path fromLocal = new Path("/Users/xuerong/Desktop/dictator_workspace/ebigdata/pom.xml");
        Path to= new Path("/hdfsapi/test/");
        fileSystem.copyFromLocalFile(fromLocal, to);
    }

    @Test
    public void copyFromBigLocal() throws Exception {
        Path to= new Path("/hdfsapi/test/pom.a");
        InputStream in = new BufferedInputStream(new FileInputStream(new File("/Users/xuerong/Desktop/dictator_workspace/ebigdata/pom.xml")));
        FSDataOutputStream out = fileSystem.create(to,
            new Progressable() {
                public void progress() {
                    System.out.println(".");
                }
            }
        );
        IOUtils.copyBytes(in, out, 4096);
    }

    public static void main(String[] args) throws Exception {

    }

}
