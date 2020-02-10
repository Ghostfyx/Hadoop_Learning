package com.hadoop.learning.chap03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * @Description: 使用FileSystem的getFileStatus方法获取FileStatus对象
 * @Author: FanYueXiang
 * @Date: 2020/2/10 7:27 AM
 */
public class ShowFileStatus {

    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), configuration);
        FileStatus fileStatus = fs.getFileStatus(new Path(uri));
        // 获取文件长度
        System.out.println(fileStatus.getLen());
        System.out.println(fileStatus.getPath());
        // 获取复制的副本数量
        System.out.println(fileStatus.getReplication());
        // 最后更改时间
        System.out.println(fileStatus.getModificationTime());
        // BlockSize
        System.out.println(fileStatus.getBlockSize());
        System.out.println(fileStatus.getOwner());
        System.out.println(fileStatus.getGroup());
    }

}
