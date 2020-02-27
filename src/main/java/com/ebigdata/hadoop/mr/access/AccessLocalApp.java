package com.ebigdata.hadoop.mr.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

public class AccessLocalApp {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(AccessLocalApp.class);

        job.setMapperClass(AccessMapper.class);
        job.setReducerClass(AccessReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Access.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Access.class);

        FileInputFormat.setInputPaths(job, new Path(""));
        FileOutputFormat.setOutputPath(job, new Path(""));

        job.waitForCompletion(true);
    }
}
