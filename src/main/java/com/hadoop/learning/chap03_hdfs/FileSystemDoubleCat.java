package com.hadoop.learning.chap03_hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;

/**
 * @Description: 使用FSDataFileSystem的seek方法读取文件两遍
 * @Author: FanYueXiang
 * @Date: 2020/2/4
 */
public class FileSystemDoubleCat {

    public static void main(String args[]){
        Configuration conf = new Configuration();
        String url = args[0];
        FSDataInputStream in = null;
        try {
            FileSystem fs = FileSystem.get(URI.create(url),conf);
            in = fs.open(new Path(url));
            IOUtils.copyBytes(in, System.out, 4096, false);
            // 返回文件起始位置
            in.seek(0);
            IOUtils.copyBytes(in, System.out, 4096, false);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(in);
        }
    }

}
