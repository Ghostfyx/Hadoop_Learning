package com.hadoop.learning.chap05_io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Properties;

/**
 * @description: 应用根据文件扩展名选取codec解压缩文件，并将文件解压存储
 * @author: fanyeuxiang
 * @createDate: 2020-04-01
 */
public class FileDecompressor {

    public static void main(String[] args) throws Exception{
        String uri = args[0];
        Properties properties = System.getProperties();
        properties.setProperty("HADOOP_USER_NAME", "root");
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI(uri), configuration);
        Path inputPath = new Path(uri);

        CompressionCodecFactory compressionCodecFactory = new CompressionCodecFactory(configuration);
        CompressionCodec codec = compressionCodecFactory.getCodec(inputPath);
        if (codec == null) {
            System.err.println("No codec found for " + uri);
            System.exit(1);
        }

        String outputUri = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());

        InputStream in = null;
        OutputStream out = null;
        try {
            in = codec.createInputStream(fs.open(inputPath));
            out = fs.create(new Path(outputUri));
            IOUtils.copyBytes(in, out, configuration);
        }finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }
    }

}
