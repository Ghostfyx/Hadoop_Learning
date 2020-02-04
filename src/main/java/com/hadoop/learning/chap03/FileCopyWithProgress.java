package com.hadoop.learning.chap03;

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
import java.util.Date;

/**
 * @Description: 将本地文件Copy到HDFS上
 * @Author: FanYueXiang
 * @Date: 2020/2/4 12:52 PM
 */
public class FileCopyWithProgress {

    public static void main(String[] args) throws Exception {
        String localdir = args[0];
        String dst = args[1];
        InputStream in = new BufferedInputStream(new FileInputStream(localdir));
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), configuration);
        OutputStream outputStream = fs.create(new Path(dst), new Progressable() {
            @Override
            public void progress() {
                System.out.println(new Date());
            }
        });
        IOUtils.copyBytes(in, System.out, 4096, true);
    }

}
