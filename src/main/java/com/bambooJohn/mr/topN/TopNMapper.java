package com.bambooJohn.mr.topN;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description:
 * @Author: liangzhen
 * @Email: 1033855573@qq.com
 * @Date: 2020-12-07 19:15
 */
public class TopNMapper extends Mapper<LongWritable,Text,FlowBean, NullWritable> {

    private FlowBean flowBean = new FlowBean();
    //private Text phone = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split("\t");


       // phone.set(fields[0]);
        /*flowBean.setUpFlow(Long.parseLong(fields[1]));
        flowBean.setDownFlow(Long.parseLong(fields[2]));
        flowBean.setSumFlow(Long.parseLong(fields[3]));*/

        flowBean.setPhone(fields[0]);
        flowBean.set(
                Long.parseLong(fields[1]),//上行·
                Long.parseLong(fields[2])//下行
        );

        context.write(flowBean,NullWritable.get());
    }
}
