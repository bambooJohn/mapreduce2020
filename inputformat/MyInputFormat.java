package com.bambooJohn.mr.inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @Description:
 * @Author: liangzhen
 * @Email: 1033855573@qq.com
 * @Date: 2020-11-09 18:17
 */
public class MyInputFormat extends FileInputFormat<Text, BytesWritable> {


    /**
     * @Description: 返回一个自定义的RecordReader
     * @Author: liangzhen
     * @Email: 1033855573@qq.com
     * @create: 2020/11/9 18:20
     * @Param:
     * @return:
     */
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        return new MyRecordReader();

    }
}
