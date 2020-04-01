package com.hadoop.learning.chap05_io;

import com.hadoop.learning.chap02_mr_introduction.MaxTemperature;
import com.hadoop.learning.chap02_mr_introduction.MaxTemperatureMapper;
import com.hadoop.learning.chap02_mr_introduction.MaxTemperatureReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Properties;

/**
 * @description: 执行MapReduce作业将输出结果保存为SequenceFile文件
 * @author: fanyeuxiang
 * @createDate: 2020-04-01
 */
public class MaxTemperatureWithSequenceFIle extends Configured implements Tool{

    @Override
    public int run(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];

        Properties properties = System.getProperties();
        properties.setProperty("HADOOP_USER_NAME", "root");
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperatureWithCompression <input path> " +
                    "<output path>");
            System.exit(-1);
        }
        Job job = Job.getInstance();
        job.setJarByClass(MaxTemperatureWithSequenceFIle.class);
        job.setJobName("Max temperature");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        job.setCombinerClass(MaxTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        boolean status = job.waitForCompletion(true);
        System.out.println("run(): status="+status);
        return status ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int returnStatus = ToolRunner.run(new MaxTemperatureWithSequenceFIle(), args);
        System.exit(returnStatus);
    }

}
