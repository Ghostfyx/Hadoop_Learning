package com.hadoop.learning.chap03_hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * @Description: 使用FileUtil的stat2Paths方法将FileStatus对象数组转换为Path对象数组
 * @Author: FanYueXiang
 * @Date: 2020/2/10
 */
public class FileStatus2Path {

    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), configuration);
        Path[] paths = new Path[args.length];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = new Path(args[i]);
        }
        FileStatus[] statuses = fs.listStatus(paths);
        Path[] pathsTransform = FileUtil.stat2Paths(statuses);
        for (Path p : pathsTransform){
            System.out.println(p.getName());
        }
    }

}
