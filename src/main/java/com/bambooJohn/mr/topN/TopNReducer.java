package com.bambooJohn.mr.topN;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Description:
 * @Author: liangzhen
 * @Email: 1033855573@qq.com
 * @Date: 2020-12-07 19:14
 */
public class TopNReducer extends Reducer<FlowBean,NullWritable,FlowBean,NullWritable> {

   // private static int count = 0;

  //  private Text phone = new Text();
    private FlowBean flowBean = new FlowBean();

    /**
     * @Description: 取前十
     * @Author: liangzhen
     * @Email: 1033855573@qq.com
     * @create: 2020/12/7 21:33
     * @Param: 所有数据一组进来，按照流量降序排序
     * @return:
     */
    @Override
    protected void reduce(FlowBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        Iterator<NullWritable> iterator = values.iterator();
        /*while(iterator.hasNext()){
            if(count < 10){
                count++;
                phone.set(iterator.next());
                context.write(phone,key);
            }else {
                return;
            }
        }*/

        for (int i = 0; i < 10; i++) {
            if(iterator.hasNext()){
                context.write(key, iterator.next());
            }
        }

    }
}
