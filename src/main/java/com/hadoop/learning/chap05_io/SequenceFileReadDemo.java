package com.hadoop.learning.chap05_io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.net.URI;

/**
 * @description: SequenceFile 数据读取demo
 * @author: fanyeuxiang
 * @createDate: 2020-03-04
 */
public class SequenceFileReadDemo {

    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), configuration);
        Path path = new Path(uri);

        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(fs, path, configuration);
            Writable key = (Writable)
                    ReflectionUtils.newInstance(reader.getKeyClass(), configuration);
            Writable value = (Writable)
                    ReflectionUtils.newInstance(reader.getValueClass(), configuration);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
