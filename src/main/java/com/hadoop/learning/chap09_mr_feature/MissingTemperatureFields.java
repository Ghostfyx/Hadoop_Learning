package com.hadoop.learning.chap09_mr_feature;

import com.hadoop.learning.common.JobBuilder;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Description: 统计气温信息缺失记录所占的比例
 * @Author: FanYueXiang
 * @Date: 2020/4/21 11:26 PM
 */
public class MissingTemperatureFields extends Configured implements Tool{

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 1) {
            JobBuilder.printUsage(this, "<job ID>");
            return -1;
        }
        String jobID = args[0];
        Cluster cluster = new Cluster(getConf());
        Job job = cluster.getJob(JobID.forName(jobID));
        /**
         * 多种因素无法从集群中找到Job，例如：
         * 1.JobId制定错误;
         * 2.Job不再保留在历史服务器中，则无法找到Job
         */
        if (job == null) {
            System.err.printf("No job with ID %s found.\n", jobID);
            return -1;
        }
        /**
         * 通常会在作业运行完成后进行统计处理；
         * 也可以在作业运行期间获取作业计数器
         */
        if (!job.isComplete()){
            System.err.printf("Job %s is not complete.\n", jobID);
            return -1;
        }
        Counters counters = job.getCounters();
        long missing = counters.findCounter(
                MaxTemperatureWithCounters.TemperatureEnum.MISSING).getValue();
        // 获取map任务计数器
        long total = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
        System.out.printf("Records with missing temperature fields: %.2f%%\n",
                100.0 * missing / total);
        return 0;
    }

    public void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(this, args);
        System.exit(exitCode);
    }
}
