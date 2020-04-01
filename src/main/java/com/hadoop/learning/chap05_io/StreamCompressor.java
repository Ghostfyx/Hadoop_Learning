package com.hadoop.learning.chap05_io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.net.URI;

/**
 * @description: 压缩从标准输入读取的数据，然后将其写到标准输出
 * @author: fanyeuxiang
 * @createDate: 2020-04-01
 */
public class StreamCompressor {

    public static void main(String[] args) throws Exception {
        String codecClassName = args[0];
        String inputFilePath = args[1];
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI(inputFilePath), configuration);
        Class<?> codecClass = Class.forName(codecClassName);
        FSDataInputStream in = fs.open(new Path(inputFilePath));
        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, configuration);
        CompressionOutputStream out = codec.createOutputStream(System.out);
        IOUtils.copyBytes(in, out, 4096, false);
        out.finish();
    }

}
