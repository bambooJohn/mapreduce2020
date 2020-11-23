package com.bambooJohn.mr.inputformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Description: 负责将整个文件转化为一组Key Value对
 * @Author: liangzhen
 * @Email: 1033855573@qq.com
 * @Date: 2020-11-09 18:19
 */
public class MyRecordReader extends RecordReader<Text, BytesWritable> {

    //表示文件读完了，默认是false，表示文件没读过
    private boolean isRead = false;

    //kv对
    private Text key = new Text();
    private BytesWritable value = new BytesWritable();

    private FSDataInputStream inputStream;
    private FileSplit fs;

    /**
     * @Description: 初始化方法，一般执行一些初始化操作
     * @Author: liangzhen
     * @Email: 1033855573@qq.com
     * @create: 2020/11/9 19:04
     * @Param:
     * @return:
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        //开流
        fs = (FileSplit) split;
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        inputStream = fileSystem.open(fs.getPath());

    }

    /**
     * @Description: 读取下一组key value Pair
     * @Author: liangzhen
     * @Email: 1033855573@qq.com
     * @create: 2020/11/9 18:58
     * @Param: 
     * @return: 
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if(!isRead){
            //读取这个文件
            //填充key
            key.set(fs.getPath().toString());

            //填充value
            byte[] buffer = new byte[((int) fs.getLength())];
            inputStream.read(buffer);

            value.set(buffer,0,buffer.length);
            //标记文件读完
            isRead = true;
            return true;
        }else {
            return false;
        }
    }

    /**
     * @Description: 获取当前读到的key
     * @Author: liangzhen
     * @Email: 1033855573@qq.com
     * @create: 2020/11/9 18:58
     * @Param: 
     * @return: 
     */
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    /**
     * @Description: 获取当前读到的value
     * @Author: liangzhen
     * @Email: 1033855573@qq.com
     * @create: 2020/11/9 18:59
     * @Param:
     * @return:
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * @Description: 显示进度
     * @Author: liangzhen
     * @Email: 1033855573@qq.com
     * @create: 2020/11/9 19:02
     * @Param:
     * @return:
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return isRead ? 1 : 0;
    }

    @Override
    public void close() throws IOException {
        //关流

        IOUtils.closeStream(inputStream);

    }
}
