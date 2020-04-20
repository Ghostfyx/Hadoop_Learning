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
 * @Description: MultipleOutputs将数据写到多个文件
 * @Author: FanYueXiang
 * @Date: 2020/4/20 10:38 PM
 */
public class PartitionByStationUsingMultipleOutputs extends Configured implements Tool {

    static class StationMapper extends Mapper<LongWritable, Text, Text, Text>{
        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            context.write(new Text(parser.getStationId()), value);
        }
    }

    static class MultipleOutputsReducer
            extends Reducer<Text, Text, NullWritable, Text> {
        private MultipleOutputs<NullWritable, Text> multipleOutputs;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                // 输出以每个站点的气象记录数据，每个站点一个文件
                multipleOutputs.write(NullWritable.get(), value, key.toString());
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (job == null) {
            return -1;
        }
        job.setMapperClass(StationMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setReducerClass(MultipleOutputsReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new PartitionByStationUsingMultipleOutputs(),
                args);
        System.exit(exitCode);
    }
}
