package com.bambooJohn.mr.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description:
 * @Author: liangzhen
 * @Email: 1033855573@qq.com
 * @Date: 2020-11-05 19:41
 */
public class FlowReducer extends Reducer<Text, FlowBean,Text,FlowBean> {

    private Text phone = new Text();
    private FlowBean flow = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //累加流量
        long upFlow = 0L;
        long downFlow = 0L;

        for (FlowBean value : values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
        }
        phone.set(key);

        //封装Flow类型
        flow.set(upFlow , downFlow);

        context.write(phone,flow);

    }
}
