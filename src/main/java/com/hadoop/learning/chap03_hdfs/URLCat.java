package com.hadoop.learning.chap03_hdfs;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URL;

/**
 * @Description: 从Hadoop URL读取数据
 * @Author: FanYueXiang
 * @Date: 2020/3/29 9:51 AM
 */
public class URLCat {

    /**
     * 设置处理Java URL流对象，每个Java 虚拟机只能调用用此这个方法
     */
    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void main(String[] args){
        InputStream in = null;
        try {
            in = new URL(args[0]).openStream();
            // 设置缓冲区大小为4096
            IOUtils.copyBytes(in, System.out,4096,false);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(in);
        }
    }
}
