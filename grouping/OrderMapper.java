package com.bambooJohn.mr.grouping;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description:
 * @Author: liangzhen
 * @Email: 1033855573@qq.com
 * @Date: 2020-11-19 0:07
 */
public class OrderMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {

    private OrderBean order = new OrderBean();

    /**
     * @Description: Map用来封装OrderBean
     * @Author: liangzhen
     * @Email: 1033855573@qq.com
     * @create: 2020/11/19 0:11
     * @Param:
     * @return:
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split("\t");

        order.setOrderId(fields[0]);
        order.setProductId(fields[1]);
        order.setPrice(Double.parseDouble(fields[2]));

        context.write(order,NullWritable.get());

    }
}
