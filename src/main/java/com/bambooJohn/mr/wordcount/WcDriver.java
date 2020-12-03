package com.bambooJohn.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Description:
 * @Author: liangzhen
 * @Email: 1033855573@qq.com
 * @Date: 2020-11-04 16:45
 */
public class WcDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //1.设置Job实例
        Configuration configuration = new Configuration();

        configuration.set("fs.defaultFS","hdfs://hadoop102:8020");
        configuration.set("mapreduce.framework.name","yarn");
        configuration.set("mapreduce.app-submission.cross-platform","true"); // 允不允许从windows向Linux提交任务
        configuration.set("yarn.resourcemanager.hostname","hadoop103");


        configuration.set("mapred.job.queue.name","hive");

        configuration.setBoolean("mapreduce.map.output.compress",true);
        configuration.setClass("mapreduce.map.output.compress.code", SnappyCodec.class, CompressionCodec.class);

        configuration.setBoolean("mapreduce.output.fileoutputformat.compress",true);
        configuration.setClass("mapreduce.output.fileoutputformat.compress.code", SnappyCodec.class, CompressionCodec.class);

        Job job = Job.getInstance(configuration);

        //2.设置Jar包
     //   job.setJarByClass(WcDriver.class);
        job.setJar("D:\\programs\\JavaProject\\IdeaProjects\\mapreduce2020\\target\\mapreduce2020-1.0-SNAPSHOT.jar");

        //3.设置Mapper和Reducer
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReducer.class);

        job.setCombinerClass(WcReducer.class);

        //4.设置Map和Reudce的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

      //  job.setNumReduceTasks(3);
        //5.设置输入输出文件
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
     //   FileInputFormat.setInputPaths(job,new Path("D:\\input"));
     //   FileOutputFormat.setOutputPath(job,new Path("D:\\output"));

        //6.提交Job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }

}
