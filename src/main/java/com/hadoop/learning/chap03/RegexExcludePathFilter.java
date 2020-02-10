package com.hadoop.learning.chap03;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * @Description: 实现PathFiler接口的过滤器
 * @Author: FanYueXiang
 * @Date: 2020/2/10 8:44 AM
 */
public class RegexExcludePathFilter implements PathFilter {

    private final String regex;

    public RegexExcludePathFilter(String regex) {
        this.regex = regex;
    }

    @Override
    public boolean accept(Path path) {
        return !path.toString().matches(regex);
    }
}
