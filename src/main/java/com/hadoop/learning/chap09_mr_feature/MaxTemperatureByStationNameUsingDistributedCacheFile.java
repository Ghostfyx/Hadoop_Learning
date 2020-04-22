package com.hadoop.learning.chap09_mr_feature;

import com.hadoop.learning.chap06_mr_dev.v2.NcdcRecordParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;

/**
 * @Description: 在与作业配置中序列化边数据的技术相比，Hadoop分布式缓存机制更受青睐，
 * 它能够在任务运行过程中及时地将文件和存档复制到任务节点以供使用。
 * 为了节约网络带宽，在每一个作业中，各个文件通常只需复制到一个节点一次
 * @Author: FanYueXiang
 * @Date: 2020/4/22 11:54 PM
 */
public class MaxTemperatureByStationNameUsingDistributedCacheFile extends Configured implements Tool {

    /**
     * 当用户启动一个作业，Hadoop会把由-files、-archives和-libjars等选项所指定的文件复制到分布式文件系统(一般是HDFS)之中。
     * 接着，在任务运行之前，YARN中NodeManager将文件从分布式文件系统复制到本地磁盘(缓存)使任务能够访问文件
     */

    static enum StationFile{STATION_SIZE};

    static class StationTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            parser.parse(value);
            if (parser.isValidTemperature()) {
                context.write(new Text(parser.getStationId()),
                        new IntWritable(parser.getAirTemperature()));
            }
        }
    }

    static class MaxTemperatureReducerWithStationLookup
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private NcdcStationMetadata metadata;/*]*/

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            metadata = new NcdcStationMetadata();
            metadata.initialize(new File("stations-fixed-width.txt"));
            // 分布式缓存文件计数器
            context.getCounter(StationFile.STATION_SIZE).setValue(metadata.getStationMap().size());
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            String stationName = metadata.getStationName(key.toString());
            int maxValue = Integer.MIN_VALUE;
            for (IntWritable value : values) {
                maxValue = Math.max(maxValue, value.get());
            }
            context.write(new Text(stationName), new IntWritable(maxValue));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("MaxTemperatureByStationNameUsingDistributedCacheFile");
        job.setJarByClass(getClass());

        job.setMapperClass(StationTemperatureMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MaxTemperatureReducerWithStationLookup.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        try {
            String[] params = new String[] {
                    "hdfs://fz/user/hdfs/MapReduce/data/sideData/distributedCache/input",
                    "hdfs://fz/user/hdfs/MapReduce/data/sideData/distributedCache/output"
            };
            int exitCode = ToolRunner.run(new MaxTemperatureByStationNameUsingDistributedCacheFile(), params);
            System.exit(exitCode);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
