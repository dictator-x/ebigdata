package com.ebigdata.hadoop.mr.join;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

import java.util.List;
import java.util.ArrayList;

public class ReduceJoinApp {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(ReduceJoinApp.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DataInfo.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        MultipleInputs.addInputPath(job, new Path("input/join/input/emp.txt"), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("input/join/input/dept.txt"), TextInputFormat.class);

        Path outputDir = new Path("input/join/output");
        outputDir.getFileSystem(configuration).delete(outputDir, true);
        FileOutputFormat.setOutputPath(job, new Path("input/join/ouput"));

        job.waitForCompletion(true);
    }

    public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, DataInfo> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] splits = value.toString().split("\t");
            int length = splits.length;

            if ( length == 3 ) {

                int deptno = Integer.parseInt(splits[0]);
                String dname = splits[1];

                DataInfo dataInfo = new DataInfo();
                dataInfo.setData(dname);
                dataInfo.setFlag("d");

                context.write(new IntWritable(deptno), dataInfo);
            } else if ( length == 8 ) {
                String empno = splits[0];
                String ename = splits[1];
                String sal = splits[5];
                int deptno = Integer.parseInt(splits[7]);
                StringBuilder builder = new StringBuilder();
                builder
                    .append(empno).append("\t")
                    .append(ename).append("\t")
                    .append(sal).append("\t");
                DataInfo dataInfo = new DataInfo();
                dataInfo.setData(builder.toString());
                dataInfo.setFlag("e");

                context.write(new IntWritable(deptno), dataInfo);
            }
        }
    }


    public static class MyReducer extends Reducer<IntWritable, DataInfo, Text, NullWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<DataInfo> values, Context context) throws IOException, InterruptedException {
            List<String> emps = new ArrayList<String>();
            List<String> depts = new ArrayList<String>();

            for ( DataInfo dataInfo : values ) {
                if ( "e".equals(dataInfo.getFlag()) ) {
                    emps.add(dataInfo.getData());
                } else if ( "d".equals(dataInfo.getFlag()) ) {
                    depts.add(dataInfo.getData());
                }
            }

            int i, j;

            for ( i = 0 ; i < emps.size() ; i++ ) {
                for ( j = 0 ; j < depts.size() ; j++ ) {
                    context.write(new Text(emps.get(i) + "\t" + depts.get(j)), NullWritable.get());
                }
            }
        }
    }
}
