package com.hadoop.learning.chap03_hdfs;

import org.apache.hadoop.conf.Configuration;

import java.util.Iterator;
import java.util.Map;

/**
 * @Description: 显示hadoop默认配置
 * @Author: FanYueXiang
 * @Date: 2020/3/29 3:25 PM
 */
public class PrintHadoopDefaultConf {

    public static void main(String[] args){
        Configuration conf = new Configuration();
        System.out.println("Hadoop configuration:"+conf.toString());
        for (Iterator<Map.Entry<String, String>> it = conf.iterator(); it.hasNext(); ) {
            Map.Entry<String, String> entry = it.next();
            System.out.println(entry.getKey()+"："+entry.getValue());
        }
    }

}
