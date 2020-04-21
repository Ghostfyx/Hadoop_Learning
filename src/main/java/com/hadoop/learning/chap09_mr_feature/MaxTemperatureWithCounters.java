package com.hadoop.learning.chap09_mr_feature;

import com.hadoop.learning.chap06_mr_dev.v1.MaxTemperatureReducer;
import com.hadoop.learning.chap06_mr_dev.v2.NcdcRecordParser;
import com.hadoop.learning.common.JobBuilder;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @Description: 用户自定义计数器
 * @Author: FanYueXiang
 * @Date: 2020/4/21 11:14 PM
 */
public class MaxTemperatureWithCounters extends Configured implements Tool{

    enum TemperatureEnum {
        MISSING,
        MALFORMED
    }
    static class MaxTemperatureMapperWithCounters
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            if (parser.isValidTemperature()){
                int airTemperature = parser.getAirTemperature();
                context.write(new Text(parser.getYear()),
                        new IntWritable(airTemperature));
            }else if(parser.isMalformedTemperature()){
                System.err.println("Ignoring possibly corrupt input: " + value);
                context.getCounter(TemperatureEnum.MALFORMED).increment(1);
            }else if (parser.isMissingTemperature()){
                context.getCounter(TemperatureEnum.MISSING).increment(1);
            }
            // dynamic counter 不由Java枚举类型定义的计数器
            context.getCounter("TemperatureQuality", parser.getQuality()).increment(1);
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);

        job.setMapperClass(MaxTemperatureMapperWithCounters.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MaxTemperatureWithCounters(), args);
        System.exit(exitCode);
    }
}
