package com.bambooJohn.mr.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description:
 * @Author: liangzhen
 * @Email: 1033855573@qq.com
 * @Date: 2020-11-05 19:40
 */
public class FlowMapper extends Mapper<LongWritable,Text, Text, FlowBean> {

    private Text phone = new Text();
    private FlowBean flow = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //拿到一行数据
        String line = value.toString();
        //切分
        String[] fields = line.split("\t");

        //封装
        phone.set(fields[1]);

        flow.set(
                Long.parseLong(fields[fields.length - 3]),
                Long.parseLong(fields[fields.length - 2])
                );

        context.write(phone,flow);

    }
}
