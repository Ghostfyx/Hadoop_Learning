package com.hadoop.learning.chap09_mr_feature;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description:
 * @Author: FanYueXiang
 * @Date: 2020/4/22 1:00 AM
 */
public class IntPair implements WritableComparable<IntPair> {
    int first;
    int second;

    /**
     *
     */
    public IntPair() {
        //这里是必须加上的，即使是一个空方法，虽然java默认有一个空的构造函数，但还是要写出来，因为
        //所有Writable实现都必须有一个默认构造函数以便MR框架可以对它们进行实例化
        //如果不加，会出现java.lang.NoSuchMethodException: com.maxtemperature.Util.IntPair.<init>()
    }

    /**
     * @param first
     * @param second
     */
    public IntPair(int first, int second) {
        super();
        this.first = first;
        this.second = second;
    }

    /**
     * 依次将每个int对象序列化到输入流中
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeInt(second);
    }

    /**
     * 查看（填充)各个字段的值
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readInt();
        second = in.readInt();
    }

    //这个地方对数据进行排序，前递增后递减
    @Override
    public int compareTo(IntPair pair) {
        int cmpFirst=Integer.valueOf(first).compareTo(pair.first);
        if (cmpFirst != 0) {
            return cmpFirst;
        } else{
            return Integer.valueOf(second).compareTo(pair.second);
        }
    }


    @Override
    public int hashCode() {
        return first * 163 + second;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof IntPair){
            IntPair ip=(IntPair)obj;
            return first == ip.getFirst() && second ==ip.getSecond();
        }
        return false;
    }

    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    /**
     * 这里决定了最终写到文件里的内容
     * @return
     */
    @Override
    public String toString() {
        return  first+"\t"+ second;
    }

    /**
     * 案例中用到的compare方法
     * @param first1
     * @param first2
     * @return
     */
    public static int compare(int first1, int first2) {
        return Integer.valueOf(first1).compareTo(first2);
    }
}
