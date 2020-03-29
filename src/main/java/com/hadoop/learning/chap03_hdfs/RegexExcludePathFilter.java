package com.hadoop.learning.chap03_hdfs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * @Description: FileSystem的listStatus()和globalStatus()方法提供了可选的PathFilter对象
 * @Author: FanYueXiang
 * @Date: 2020/3/29 3:39 PM
 */
public class RegexExcludePathFilter implements PathFilter {

    /**
     * 可以使用正则表达式
     */
    private final String regex;

    public RegexExcludePathFilter(String regex) {
        this.regex = regex;
    }

    @Override
    public boolean accept(Path path) {
        return !path.toString().matches(regex);
    }
}
