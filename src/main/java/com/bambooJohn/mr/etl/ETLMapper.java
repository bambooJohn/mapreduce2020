package com.bambooJohn.mr.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description:
 * @Author: liangzhen
 * @Email: 1033855573@qq.com
 * @Date: 2020-11-28 9:13
 */
public class ETLMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    private Counter pass;
    private Counter fail;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    //    pass = context.getCounter("ETL", "Pass");
    //    fail = context.getCounter("ETL", "Fail");

        pass = context.getCounter(ETL.PASS);
        fail = context.getCounter(ETL.FAIL);

    }

    /**
     * @Description: 判断日志需不需要清洗
     * @Author: liangzhen
     * @Email: 1033855573@qq.com
     * @create: 2020/11/28 9:27
     * @Param: key  value 一行日志
     * @return:
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //切分
        String[] fields = value.toString().split(" ");

        //自由长度大于11的我们才要
        if(fields.length > 11){
            pass.increment(1L);
            context.write(value,NullWritable.get());
        }else{
            fail.increment(1L);
        }

    }
}
