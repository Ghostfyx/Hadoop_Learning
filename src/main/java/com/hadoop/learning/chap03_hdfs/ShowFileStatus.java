package com.hadoop.learning.chap03_hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Description: 使用FileSystem的getFileStatus方法获取FileStatus对象
 * @Author: FanYueXiang
 * @Date: 2020/2/10
 */
public class ShowFileStatus {

    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), configuration);
        FileStatus fileStatus = fs.getFileStatus(new Path(uri));
        // 获取文件长度
        System.out.println("fileLength: "+fileStatus.getLen());
        System.out.println("filePath: "+fileStatus.getPath());
        // 获取复制的副本数量
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println("file replication: "+fileStatus.getReplication());
        // 最后更改时间
        System.out.println("file mdificationTime: "+format.format(new Date(fileStatus.getModificationTime())));
        // BlockSize
        System.out.println("file BlockSize: "+fileStatus.getBlockSize());
        System.out.println("file Owner: "+fileStatus.getOwner());
        System.out.println("file Group: "+fileStatus.getGroup());
    }

}
