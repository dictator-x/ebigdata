package com.ebigdata.hadoop.mr.join;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;

import java.util.Map;
import java.util.HashMap;

import java.net.URI;

public class MapJoinApp {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(MapJoinApp.class);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);


        // load small file into cache.
        job.addCacheFile(new URI("/samelfilelocation"));
        FileInputFormat.setInputPaths(job, new Path("input/join/input/emp.txt"));

        Path outputDir = new Path("input/join/output");
        outputDir.getFileSystem(configuration).delete(outputDir, true);
        FileOutputFormat.setOutputPath(job, new Path("input/join/ouput"));

        job.waitForCompletion(true);
    }

    public static class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        private static Map<Integer, String> cache = new HashMap<Integer, String>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String path = context.getCacheFiles()[0].toString();
            BufferedReader reader = new BufferedReader(new FileReader(path));

            String line = null;
            while ( ( line = reader.readLine() ) != null ) {
                String[] splits = line.split("\t");
                int deptno = Integer.parseInt(splits[0]);
                String dname = splits[1];

                cache.put(deptno, dname);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] splits = value.toString().split("\t");
            int length = splits.length;

            if ( length == 8 ) {
                String empno = splits[0];
                String ename = splits[1];
                String sal = splits[5];
                int deptno = Integer.parseInt(splits[7]);

                String dname = cache.get(deptno);

                StringBuilder builder = new StringBuilder();
                builder
                    .append(empno).append("\t")
                    .append(ename).append("\t")
                    .append(sal).append("\t")
                    .append(dname).append("\t");

                context.write(new Text(builder.toString()), NullWritable.get());
            }
        }
    }
}
