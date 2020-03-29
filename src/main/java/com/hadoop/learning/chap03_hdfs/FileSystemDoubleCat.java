package com.hadoop.learning.chap03_hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.net.URI;

/**
 * @Description: fs.open方法返回FSDataInputStream，使用FSDataInputStream任意位置
 * 读取数据特点，读取两遍文件
 * @Author: FanYueXiang
 * @Date: 2020/3/29 10:06 AM
 */
public class FileSystemDoubleCat {

    public static void main(String[] args) throws Exception{
        FSDataInputStream in = null;
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(uri), conf);
        try{
            in = fileSystem.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
            // 返回文件起始位置，seek方法跳转到文件任意位置
            in.seek(0);
            IOUtils.copyBytes(in, System.out, 4096, false);
        }finally {
            IOUtils.closeStream(in);
        }
    }

}
