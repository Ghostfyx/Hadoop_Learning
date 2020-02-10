package com.hadoop.learning.chap03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @Description: 使用FileSystem的delete方法永久删除文件
 * @Author: FanYueXiang
 * @Date: 2020/2/10 8:55 AM
 */
public class DelateFile {

    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);
        fs.delete(new Path(uri));
    }

}
