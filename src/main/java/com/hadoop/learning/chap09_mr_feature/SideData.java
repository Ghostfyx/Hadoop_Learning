package com.hadoop.learning.chap09_mr_feature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @Description: “边数据”(side data)是作业所需的额外只读数据，以辅助处理主数据集。
 * @Author: FanYueXiang
 * @Date: 2020/4/22 11:37 PM
 */
public class SideData extends Configured implements Tool {

    /**
     * 利用JobConf来配置作业，这种机制并不适合传输多达几千字节的数据量。
     * 每次读取配置时，所有项都被读入到内存(即使暂时不用的属性项也不例外)。
     */
    static class SideDataMapper extends Mapper<LongWritable, Text, Text, Text> {
        String sideDataValue = "";

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            sideDataValue = context.getConfiguration().get("sideData_test_key");
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            context.write(value, new Text(sideDataValue));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("sideData_test_key", "sideData_test_value");
        Job job = Job.getInstance(configuration);
        job.setJobName("SideData");
        job.setJarByClass(getClass());
        job.setMapperClass(SideDataMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        try {
            String[] params = new String[]{
                    "hdfs://fz/user/hdfs/MapReduce/data/sideData/job/input",
                    "hdfs://fz/user/hdfs/MapReduce/data/sideData/job/output"
            };
            int exitCode = ToolRunner.run(new SideData(), params);
            System.exit(exitCode);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
