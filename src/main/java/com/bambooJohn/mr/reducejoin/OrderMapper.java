package com.bambooJohn.mr.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Description:
 * @Author: liangzhen
 * @Email: 1033855573@qq.com
 * @Date: 2020-11-25 11:17
 */
public class OrderMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {

    private OrderBean orderBean = new OrderBean();
    private String fileName = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取输入数据的文件名
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        fileName = inputSplit.getPath().getName();

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //切分一行内容
        String[] words = value.toString().split("\t");

        //封装，按照数据来源不同分别封装
        if(fileName.contains("order.txt")){
            //封装order
            orderBean.setId(words[0]);
            orderBean.setPid(words[1]);
            orderBean.setAmount(Integer.parseInt(words[2]));
            orderBean.setPname("");
        }else{
            //封装pd
            orderBean.setId("");
            orderBean.setPid(words[0]);
            orderBean.setAmount(0);
            orderBean.setPname(words[1]);
        }
        context.write(orderBean,NullWritable.get());
    }
}
