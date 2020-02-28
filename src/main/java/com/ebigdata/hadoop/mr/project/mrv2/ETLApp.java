package com.ebigdata.hadoop.mr.project.mrv2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.conf.Configuration;

import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.Map;

import com.ebigdata.hadoop.mr.project.utils.ContentUtils;
import com.ebigdata.hadoop.mr.project.utils.LogParser;

public class ETLApp {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();

        // String input = "input/raw/trackinfo_20130721.data";
        // String dest = "input/etl";

        String input = args[0];
        String dest = args[1];

        FileSystem fileSystem = FileSystem.get(configuration);
        Path outputPath = new Path(dest);

        if ( fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        Job job = Job.getInstance(configuration);
        job.setJarByClass(ETLApp.class);

        job.setMapperClass(MyMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(dest));

        job.waitForCompletion(true);
    }

    static class MyMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        private LongWritable ONE = new LongWritable(1);
        private LogParser logParser;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            logParser = new LogParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String log = value.toString();
            Map<String, String> infos = logParser.parse(log);

            String ip = infos.get("ip");
            String country = infos.get("country");
            String province = infos.get("province");
            String city = infos.get("city");
            String url = infos.get("url");
            String time = infos.get("time");
            String pageId = ContentUtils.getPageId(url);

            StringBuilder builder = new StringBuilder();
            builder.append(ip).append("\t");
            builder.append(country).append("\t");
            builder.append(province).append("\t");
            builder.append(city).append("\t");
            builder.append(url).append("\t");
            builder.append(time).append("\t");
            builder.append(pageId).append("\t");

            context.write(NullWritable.get(), new Text(builder.toString()));
        }
    }

}
