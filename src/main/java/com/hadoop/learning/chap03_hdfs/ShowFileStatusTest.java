package com.hadoop.learning.chap03_hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @Description: 查询文件/目录的元数据信息 FileStatus
 * 包括：文件长度、块大小、复本、修改时间、所有者以及权限信息
 * @Author: FanYueXiang
 * @Date: 2020/3/29 2:28 PM
 */
public class ShowFileStatusTest {

    public static void main(String[] args) throws Exception {
        String filePath = args[0];
        String dirPath = args[1];
        Configuration conf = new Configuration();
        fileStatusForFile(conf, filePath);
        fileStatusForDirectory(conf, dirPath);
    }

    /**
     * 获取文件元数据信息
     *
     * @param conf
     * @param filePath
     */
    public static void fileStatusForFile(Configuration conf, String filePath) throws Exception {
        Path file = new Path(filePath);
        FileSystem fs = FileSystem.get(conf);
        FileStatus fileStatus = fs.getFileStatus(file);
        System.out.println("file path:"+fileStatus.getPath().toUri());
        System.out.println("is directory:"+fileStatus.isDir());
        System.out.println("file length:"+fileStatus.getLen());
        System.out.println("file last modification time:"+fileStatus.getModificationTime());
        System.out.println("file replication:"+fileStatus.getReplication());
        System.out.println("file blockSize:"+fileStatus.getBlockSize());
        System.out.println("file group:"+fileStatus.getGroup());
        System.out.println("file owner:"+fileStatus.getOwner());
        System.out.println("file permission:"+fileStatus.getPermission());
    }

    /**
     * 获取目录元数据信息
     *
     * @param conf
     * @param dirPath
     * @throws Exception
     */
    public static void fileStatusForDirectory(Configuration conf, String dirPath) throws Exception{
        Path dir = new Path(dirPath);
        FileSystem fs = FileSystem.get(conf);
        FileStatus dirStatus = fs.getFileStatus(dir);
        System.out.println("directory path:"+dirStatus.getPath().toUri());
        System.out.println("is directory:"+dirStatus.isDir());
        System.out.println("directory length:"+dirStatus.getLen());
        System.out.println("directory last modification time:"+dirStatus.getModificationTime());
        System.out.println("directory replication:"+dirStatus.getReplication());
        System.out.println("directory blockSize:"+dirStatus.getBlockSize());
        System.out.println("directory group:"+dirStatus.getGroup());
        System.out.println("directory owner:"+dirStatus.getOwner());
        System.out.println("directory permission:"+dirStatus.getPermission());
    }
}
