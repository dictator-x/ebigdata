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
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.File;

import java.util.Map;
import java.util.Set;

public class HDFSWCApp01 {

    public static final String HDFS_PATH = "hdfs://hadoop000:8020";

    public static void main(String[] args) throws Exception {
        Path input = new Path("/README.txt");

        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "1");
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "hadoop");

        MapContext context = new MapContext();
        Mapper mapper = new WordCountMapper();
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(input, false);

        while ( iterator.hasNext() ) {
            LocatedFileStatus file = iterator.next();
            FSDataInputStream in = fileSystem.open(file.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            String line = "";
            while ( (line = reader.readLine()) != null ) {
                mapper.map(line, context);
            }

            reader.close();
            in.close();
        }

        Map<Object, Object> contextMap = context.getCacheMap();

        Path output = new Path("/hdfsapi/output/");

        FSDataOutputStream out = fileSystem.create(new Path(output, new Path("WC.out")));

        Set<Map.Entry<Object, Object>> entries = contextMap.entrySet();
        for ( Map.Entry<Object, Object> entry : entries ) {
            out.write((entry.getKey().toString() + "\t" + entry.getValue() + "\n").getBytes());
        }

        System.out.println("word count finish");

        out.flush();

    }
}
