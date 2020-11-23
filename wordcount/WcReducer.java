package com.bambooJohn.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description:
 * @Author: liangzhen
 * @Email: 1033855573@qq.com
 * @Date: 2020-11-04 16:45
 */
public class WcReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    private IntWritable result = new IntWritable();
    /**
     * @Description:
     * 框架把数据按照单词分好组输入给我们，我们将同一个单词的次数增加
     * @Author: liangzhen
     * @Email: 1033855573@qq.com
     * @create: 2020/11/4 18:27
     * @Param: key 单词；values 这个单词的所有的1；context 任务本身
     * @return:
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //做累加
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        //包装结果并写出去
        result.set(sum);
        context.write(key,result);

    }
}
