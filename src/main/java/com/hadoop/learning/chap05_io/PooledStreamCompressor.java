package com.hadoop.learning.chap05_io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * @description: 使用压缩池对读取自标准输入的数据进行压缩，然后将其写到标准输出
 * @author: fanyeuxiang
 * @createDate: 2020-04-01
 */
public class PooledStreamCompressor {

    public static void main(String[] args) throws Exception {
        String codecClassname = args[0];
        Class<?> codecClass = Class.forName(codecClassname);
        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec)
                ReflectionUtils.newInstance(codecClass, conf);
        Compressor compressor = null;
        try {
            // CodecPool支持反复使用压缩和解压缩，以分摊创建这些对象的开销
            compressor = CodecPool.getCompressor(codec);
            CompressionOutputStream out =
                    codec.createOutputStream(System.out, compressor);
            IOUtils.copyBytes(System.in, out, 4096, false);
            out.finish();
        }finally {
            CodecPool.returnCompressor(compressor);
        }
    }

}
