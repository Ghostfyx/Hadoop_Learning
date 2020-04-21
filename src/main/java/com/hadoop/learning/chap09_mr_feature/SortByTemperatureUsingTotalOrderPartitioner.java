package com.hadoop.learning.chap09_mr_feature;

import com.hadoop.learning.common.JobBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * @Description: 基于有序分区实现的全局文件排序
 * @Author: FanYueXiang
 * @Date: 2020/4/22 12:49 AM
 */
public class SortByTemperatureUsingTotalOrderPartitioner extends Configured
        implements Tool {

    /**
     * 具体步骤如下：
     * 1. 创建好一系列排序好的文件
     * 2. 串联这些文件
     * 3. 生成一个全局排序文件
     *
     * 实现思路：
     * 使用一个partitioner来描述输出的全局排序。类似于数据结构中的分桶排序思想
     * 例如：可以为上述文件创建4个分区，在第一个分区中，各记录的气温小于-10℃，
     * 第二分区的气温介于-10℃和0℃之间，第三个分区的气温在0℃和10℃之间，最后一个分区的气温大于10℃。
     *
     * 关键点：
     * 如何划分各个分区，理想情况下，各分区所含记录数应该大致相等，不会导致数据倾斜，个别reduce执行时间过长，
     *
     * 实现方案：
     * 通过对键空间进行采样，就可较为均匀地划分数据集，使用Hadoop中的InputSampler对原始数据集采样
     */

    @Override
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (job == null) {
            return -1;
        }

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job,
                SequenceFile.CompressionType.BLOCK);

        job.setPartitionerClass(TotalOrderPartitioner.class);
        // 使用随机采样的方式
        InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<IntWritable, Text>(0.1, 10000, 10);

        InputSampler.writePartitionFile(job, sampler);

        // Add to DistributedCache
        Configuration conf = job.getConfiguration();
        String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
        URI partitionUri = new URI(partitionFile);
        job.addCacheFile(partitionUri);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(
                new SortByTemperatureUsingTotalOrderPartitioner(), args);
        System.exit(exitCode);
    }
}
