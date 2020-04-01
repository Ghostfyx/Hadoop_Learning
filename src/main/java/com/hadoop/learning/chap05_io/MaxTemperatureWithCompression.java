package com.hadoop.learning.chap05_io;

import com.hadoop.learning.chap02_mr_introduction.MaxTemperature;
import com.hadoop.learning.chap02_mr_introduction.MaxTemperatureMapper;
import com.hadoop.learning.chap02_mr_introduction.MaxTemperatureReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Properties;

/**
 * @description: 压缩MapReduce作业输出，MapReduce会在读取文件时自动解压文件
 * @author: fanyeuxiang
 * @createDate: 2020-04-01
 */
public class MaxTemperatureWithCompression {

    public static void main(String[] args) throws Exception{
        Properties properties = System.getProperties();
        properties.setProperty("HADOOP_USER_NAME", "root");
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperatureWithCompression <input path> " +
                    "<output path>");
            System.exit(-1);
        }
        Job job = Job.getInstance();
        job.setJarByClass(MaxTemperature.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 设置数据压缩格式
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
