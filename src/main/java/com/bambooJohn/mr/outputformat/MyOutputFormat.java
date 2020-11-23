package com.bambooJohn.mr.outputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Description:返回一个处理数据的Record Writer
 * @Author: liangzhen
 * @Email: 1033855573@qq.com
 * @Date: 2020-11-24 6:07
 */
public class MyOutputFormat extends FileOutputFormat<LongWritable, Text> {

    /**
     * @Description: 返回一个处理数据的Record Writer
     * @Author: liangzhen
     * @Email: 1033855573@qq.com
     * @create: 2020/11/24 6:48
     * @Param:
     * @return:
     */
    @Override
    public RecordWriter<LongWritable, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new MyRecordWriter(job);
    }
}
