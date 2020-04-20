package com.hadoop.learning.chap08_mr_types;

import com.hadoop.learning.common.JobBuilder;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.spark.input.WholeTextFileInputFormat;

/**
 * @Description: HDFS优化之小文件合并为SequenceFile
 * @Author: FanYueXiang
 * @Date: 2020/4/20 11:44 PM
 */
public class SmallFilesToSequenceFileConverter extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job= JobBuilder.parseInputAndOutput(this,getConf(),args);

        // WholeTextFileInputFormat 将一个文件作为一条记录处理
        job.setInputFormatClass(WholeTextFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        return job.waitForCompletion(true)?0:1;
    }

    static class SequenceFileMapper extends Mapper<NullWritable,BytesWritable,Text,BytesWritable>{

    }
}
