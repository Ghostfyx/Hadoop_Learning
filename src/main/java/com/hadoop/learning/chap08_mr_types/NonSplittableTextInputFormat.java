package com.hadoop.learning.chap08_mr_types;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * @Description: 不切切分文件TextInputFormat
 * @Author: FanYueXiang
 * @Date: 2020/4/20 10:36 PM
 */
public class NonSplittableTextInputFormat extends TextInputFormat {

    @Override
    public boolean isSplitable(JobContext jobContext, Path path){
        return false;
    }

}
