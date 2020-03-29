package com.hadoop.learning.chap03_hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * @Description: 将本地文件写入到HDFS中，HDFS只允许对一个已打开的文件顺序写入或现有文件末尾追加
 * @Author: FanYueXiang
 * @Date: 2020/3/29 10:28 AM
 */
public class FileCopyWithProgress {

    public static void main(String[] args) throws Exception{
        String localSrc = args[0];
        String dst = args[1];
        InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(dst), conf);
        OutputStream out = fs.create(new Path(dst),
                new Progressable() {
            // 每次写入64KB数据写入datanode管线后，打印一个时间点
            @Override
            public void progress() {
                System.out.println(".");
            }
        });
        IOUtils.copyBytes(in, out, 4096, true);
    }

}
