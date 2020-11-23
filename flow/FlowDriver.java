package com.bambooJohn.mr.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Description:
 * @Author: liangzhen
 * @Email: 1033855573@qq.com
 * @Date: 2020-11-05 19:42
 */
public class FlowDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(FlowDriver.class);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job,new Path("D:\\input"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\output2"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0:1);

    }

}
