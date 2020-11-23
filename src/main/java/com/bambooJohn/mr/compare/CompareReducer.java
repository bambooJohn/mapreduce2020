package com.bambooJohn.mr.compare;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description:收到的数据时手机号在后，输出手机号在前
 * @Author: liangzhen
 * @Email: 1033855573@qq.com
 * @Date: 2020-11-13 21:06
 */
public class CompareReducer extends Reducer<FlowBean, Text,Text,FlowBean> {

    /**
     * @Description: Reduce收到的数据已经排完序了，我们反过来输出就完事了
     * @Author: liangzhen
     * @Email: 1033855573@qq.com
     * @create: 2020/11/13 21:12
     * @Param:
     * @return:
     */
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value : values) {
            context.write(value,key);
        }

    }
}
