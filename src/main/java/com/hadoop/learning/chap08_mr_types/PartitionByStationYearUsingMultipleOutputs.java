package com.hadoop.learning.chap08_mr_types;

import com.hadoop.learning.chap06_mr_dev.v2.NcdcRecordParser;
import com.hadoop.learning.common.JobBuilder;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @Description: 根据站点-年为分区，每个分区的数据输出为一个文件
 * @Author: FanYueXiang
 * @Date: 2020/4/20 11:17 PM
 */
public class PartitionByStationYearUsingMultipleOutputs extends Configured implements Tool {

    static class PartitionByStationYearMapper extends Mapper<LongWritable, Text, Text, Text>{
        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            context.write(new Text(parser.getStationId()), value);
        }
    }

    static class MultipleOutputsReducer extends Reducer<Text, Text, NullWritable, Text>{
        private NcdcRecordParser parser = new NcdcRecordParser();
        // 新建多文件输出类
        private MultipleOutputs<NullWritable, Text> multipleOutputs;

        /**
         * reduce任务开始是调用
         *
         * @param context
         */
        @Override
        public void setup(Context context){
            multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values){
                parser.parse(value);
                String basePath = String.format("%s/%s/part",
                        parser.getStationId(), parser.getYear());
                multipleOutputs.write(NullWritable.get(), value, basePath);
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }

    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (job == null) {
            return -1;
        }
        job.setMapperClass(PartitionByStationYearMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setReducerClass(PartitionByStationYearUsingMultipleOutputs.MultipleOutputsReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new PartitionByStationYearUsingMultipleOutputs(),
                args);
        System.exit(exitCode);
    }
}
