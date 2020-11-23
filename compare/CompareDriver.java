package com.bambooJohn.mr.compare;

import com.bambooJohn.mr.flow.FlowMapper;
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
 * @Date: 2020-11-13 21:12
 */
public class CompareDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(CompareDriver.class);

        job.setMapperClass(CompareMapper.class);
        job.setReducerClass(CompareReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setSortComparatorClass(FlowComparator.class);

        FileInputFormat.setInputPaths(job,new Path("D:\\output"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\output3"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0:1);


    }

}
