package com.hadoop.learning.chap02_mr_introduction;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-03-04
 */
public class MaxTemperature extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int returnStatus = ToolRunner.run(new MaxTemperature(), args);
        System.exit(returnStatus);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperatureWithCombiner <input path> " +
                    "<output path>");
            System.exit(-1);
        }
        System.out.println("inputDir= "+args[1]);
        System.out.println("outputDir= "+args[2]);
        Job job = Job.getInstance();
        job.setJarByClass(MaxTemperature.class);
        job.setJobName("MaxTemperature");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        boolean status = job.waitForCompletion(true);
        System.out.println("run(): status="+status);
        return status ? 0 : 1;
    }
}
