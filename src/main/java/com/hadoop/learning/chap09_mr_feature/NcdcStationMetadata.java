package com.hadoop.learning.chap09_mr_feature;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description:
 * @Author: FanYueXiang
 * @Date: 2020/4/23 12:00 AM
 */
public class NcdcStationMetadata {

    /**
     * 存放气象站ID和name
     */
    private Map<String, String> stationMap = new HashMap<String, String>();


    public Map<String, String> getStationMap() {
        return stationMap;
    }

    public void setStationMap(Map<String, String> stationMap) {
        this.stationMap = stationMap;
    }

    /**
     * 根据ID获取name
     * @param stationId
     * @return
     */
    public String getStationName(String stationId) {
        return stationMap.get(stationId);
    }

    /**
     * 解析
     * @param value
     */
    public boolean parse(String value) {
        String[] values = value.split(",");
        if (values.length >= 2) {
            String stationId = values[0];
            String stationName = values[1];
            if (null == stationMap) {
                stationMap = new HashMap<String, String>();
            }
            stationMap.put(stationId, stationName);
            return true;
        }
        return false;
    }

    /**
     * 解析气象站数据文件
     * @param file
     */
    public void initialize(File file) {

        BufferedReader reader=null;
        String temp=null;
        try{
            reader=new BufferedReader(new FileReader(file));
            System.out.println("------------------start------------------");
            while((temp=reader.readLine())!=null){
                System.out.println(temp);
                parse(temp);
            }
            System.out.println("------------------end------------------");
        }
        catch(Exception e){
            e.printStackTrace();
        }
        finally{
            if(reader!=null){
                try{
                    reader.close();
                }
                catch(Exception e){
                    e.printStackTrace();
                }
            }
        }

    }

}
