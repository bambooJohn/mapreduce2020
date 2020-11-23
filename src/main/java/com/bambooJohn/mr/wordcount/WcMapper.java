package com.bambooJohn.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description:
 * @Author: liangzhen
 * @Email: 1033855573@qq.com
 * @Date: 2020-11-04 16:45
 */
public class WcMapper extends Mapper<LongWritable,Text, Text, IntWritable> {

    private Text word = new Text();
    private IntWritable one = new IntWritable();

    /**
     * @Description:
     * 框架将数据拆成一行一行输入进来，我们把数据变成（单词,1）的形式
     * @Author: liangzhen
     * @Email: 1033855573@qq.com
     * @create: 2020/11/4 17:38
     * @Param: key 行号；value 行内容；context 任务本身
     * @return:
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //拿到一行数据
        String line = value.toString();

        // 将这一行拆成很多单词
        String[] words = line.split(" ");

        //将(单词,1)写回框架
        for (String word : words) {
            this.word.set(word);
            this.one.set(1);
            context.write(this.word,this.one);
        }
    }
}
