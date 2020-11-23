package com.bambooJohn.mr.grouping;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Description:取每个订单的最高价格
 * @Author: liangzhen
 * @Email: 1033855573@qq.com
 * @Date: 2020-11-19 0:55
 */
public class OrderReducer extends Reducer<OrderBean, NullWritable,OrderBean,NullWritable> {

    /**
     * @Description: 取每个订单前二高
     * @Author: liangzhen
     * @Email: 1033855573@qq.com
     * @create: 2020/11/19 0:56
     * @Param:
     * @return:
     */
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        Iterator<NullWritable> iterator = values.iterator();
        for (int i = 0; i < 2; i++) {
            if(iterator.hasNext()){
                context.write(key,iterator.next());
            }
        }

    }
}
