package com.hadoop.learning.chap03;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * @Description: 通过HDFS URL读取文件
 * @Author: FanYueXiang
 * @Date: 2020/2/4
 */
public class URLCat {

    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void main(String[] args){
        String filePath = args[0];
        InputStream in = null;
        try {
            in = new URL(filePath).openStream();
            IOUtils.copyBytes(in,System.out, 4096, false);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(in);
        }
    }

}
