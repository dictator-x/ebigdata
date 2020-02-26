package com.ebigdata.hadoop.hdfs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.LocatedFileStatus;
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

    @Test
    public void listFiles() throws Exception {
        FileStatus[] statuses = fileSystem.listStatus(new Path("/hdfsapi/test"));

        for ( FileStatus file : statuses ) {
            String isDir = file.isDirectory() ? "directory" : "file";
            String permission = file.getPermission().toString();
            short replication = file.getReplication();
            long length = file.getLen();
            String path = file.getPath().toString();

            System.out.println(isDir + "\t" + permission + "\t" + replication +
                "\t" + length + "\t" + path);
        }
    }

    @Test
    public void listFilesRecursive() throws Exception {
        RemoteIterator<LocatedFileStatus> files= fileSystem.listFiles(new Path("/"), true);

        while ( files.hasNext() ) {
            LocatedFileStatus file = files.next();

            String isDir = file.isDirectory() ? "directory" : "file";
            String permission = file.getPermission().toString();
            short replication = file.getReplication();
            long length = file.getLen();
            String path = file.getPath().toString();

            System.out.println(isDir + "\t" + permission + "\t" + replication +
                "\t" + length + "\t" + path);
        }
    }

    @Test
    public void copyToLocalFile() throws Exception {
        Path src = new Path("/hdfsapi/test/a.txt");
        Path dst = new Path("/Users/xuerong/Downloads");
        fileSystem.copyToLocalFile(src, dst);
    }

    @Test
    public void getFileBlockLocations() throws Exception {
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/jdk-8u91-linux-x64.tar.gz"));
        BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());

        for ( BlockLocation block : blocks ) {
            for ( String name : block.getNames() ) {
                System.out.println(name + " : " + block.getOffset() + " : " + block.getLength());
            }
        }
    }

    @Test
    public void delete() throws Exception {
        fileSystem.delete(new Path("/jdk-8u91-linux-x64.tar.gz"), true);
    }

    public static void main(String[] args) throws Exception {

    }

}
