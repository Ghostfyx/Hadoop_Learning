package com.hadoop.learning.chap09_mr_feature;

import com.hadoop.learning.common.JobBuilder;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * @Description: 通过使用map文件代替顺序文件，第一时间发现一个键所属的相关分区
 * @Author: FanYueXiang
 * @Date: 2020/4/22 12:09 AM
 */
public class SortByTemperatureToMapFile extends Configured implements Tool{

    @Override
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (job == null) {
            return -1;
        }
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        /**
         * MapFile是已经排过序的SequenceFile，它有索引，所以可以按键查找
         * 索引自身就是一个SequenceFile，包含了map中的一小部分键(默认情况下，是每隔128个键)。
         * 由于索引能够加载进内存，因此可以提供对主数据文件的快速查找。
         */
        job.setOutputFormatClass(MapFileOutputFormat.class);
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job,
                SequenceFile.CompressionType.BLOCK);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
