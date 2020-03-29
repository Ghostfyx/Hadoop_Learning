package com.hadoop.learning.chap03_hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.net.URI;


/**
 * @Description: 列出文件目录中所有文件的元数据
 * @Author: FanYueXiang
 * @Date: 2020/3/29 3:10 PM
 */
public class ListStatus {

    public static void main(String[] args) throws Exception{
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(uri), conf);
        Path[] paths = new Path[args.length];
        for (int i = 0; i < paths.length; i++){
            paths[i] = new Path(args[i]);
        }
        FileStatus[] statuses = fs.listStatus(paths);
        for (FileStatus status : statuses){
            System.out.println(status);
        }
        Path[] listPaths = FileUtil.stat2Paths(statuses);
        for (Path path : listPaths){
            System.out.println(path);
        }
    }
}
