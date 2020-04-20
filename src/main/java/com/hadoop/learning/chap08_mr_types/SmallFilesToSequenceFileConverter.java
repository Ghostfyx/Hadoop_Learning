package com.hadoop.learning.chap08_mr_types;

import com.hadoop.learning.common.JobBuilder;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * @Description: HDFS优化之小文件合并为SequenceFile
 * @Author: FanYueXiang
 * @Date: 2020/4/20 11:44 PM
 */
public class SmallFilesToSequenceFileConverter extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job= JobBuilder.parseInputAndOutput(this,getConf(),args);

        // WholeFileInputFormat 将一个文件作为一条记录处理
        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setMapperClass(SequenceFileMapper.class);
        return job.waitForCompletion(true)?0:1;
    }

    /**
     * WholeTextFileInputFormat 打开文件，产生一个长度是文件长度的字节数组
     */
    static class SequenceFileMapper extends Mapper<NullWritable,BytesWritable,Text,BytesWritable>{

        private Text filenameKey;

        @Override
        public void setup(Context context){
            // WholeTextFileInputFormat将每个文件转换为一个InputSplit
            InputSplit split=context.getInputSplit();
            Path path=((FileSplit)split).getPath();
            filenameKey=new Text(path.toString());
        }

        @Override
        public void map(NullWritable key,BytesWritable value, Context context) throws IOException, InterruptedException {
            context.write(filenameKey,value);
        }
    }
}
