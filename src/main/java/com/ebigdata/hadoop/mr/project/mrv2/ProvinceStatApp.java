package com.ebigdata.hadoop.mr.project.mrv2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Map;

import com.ebigdata.hadoop.mr.project.utils.LogParser;
import com.ebigdata.hadoop.mr.project.utils.IPParser;

public class ProvinceStatApp {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        // String input = "input/etl";
        // String dest = "output/v2/provincestat";

        String input = args[0];
        String dest = args[1];

        FileSystem fileSystem = FileSystem.get(configuration);
        Path outputPath = new Path(dest);

        if ( fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        Job job = Job.getInstance(configuration);
        job.setJarByClass(ProvinceStatApp.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(dest));

        job.waitForCompletion(true);
    }

    static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private LongWritable ONE = new LongWritable(1);
        private LogParser logParser;
        private Text nullText = new Text("-");

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            logParser = new LogParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String log = value.toString();
            Map<String, String> infos = logParser.parseV2(log);
            context.write(new Text(infos.get("province")), ONE);
        }
    }

    static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for ( LongWritable value : values ) {
                count++;
            }

            context.write(key, new LongWritable(count));
        }
    }
}
