package com.hadoop.learning.chap03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * @Description: 通过FileSystem读取HDFS文件
 * @Author: FanYueXiang
 * @Date: 2020/2/4
 */
public class FileSystemCat {

    public static void main(String args[]){
        Configuration conf = new Configuration();
        String url = args[0];
        InputStream in = null;
        try {
            FileSystem fs = FileSystem.get(URI.create(url),conf);
            in = fs.open(new Path(url));
            IOUtils.copyBytes(in, System.out, 4096, false);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(in);
        }
    }
}
