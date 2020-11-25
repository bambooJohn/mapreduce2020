package com.bambooJohn.mr.reducejoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Description:
 * 数据替换工作
 * @Author: liangzhen
 * @Email: 1033855573@qq.com
 * @Date: 2020-11-25 11:36
 */
public class OrderReducer extends Reducer<OrderBean, NullWritable,OrderBean,NullWritable> {
    
   /**
    * @Description: 收到的数据，pd的一行在开头，order的紧随其后
    * @Author: liangzhen
    * @Email: 1033855573@qq.com
    * @create: 2020/11/25 13:40
    * @Param:
    * @return:
    */
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //拿到迭代器
        Iterator<NullWritable> iterator = values.iterator();

        //迭代第一组数据
        iterator.next();
        String pname = key.getPname();

        //迭代剩下的数据写出并输出
        while(iterator.hasNext()){
            iterator.next();
            key.setPname(pname);
            context.write(key,NullWritable.get());
        }

    }
}
