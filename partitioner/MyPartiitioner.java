package com.bambooJohn.mr.partitioner;

import com.bambooJohn.mr.flow.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Description:
 * @Author: liangzhen
 * @Email: 1033855573@qq.com
 * @Date: 2020-11-13 15:11
 */
public class MyPartiitioner extends Partitioner<Text, FlowBean> {

    /**
     * @Description: 对每一条KV对，返回它们对应的分区号
     * @Author: liangzhen
     * @Email: 1033855573@qq.com
     * @create: 2020/11/13 15:16
     * @Param:
     * @return:
     */
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {

        switch(text.toString().substring(0,3)){
            case "136":
                return 0;
            case "137":
                return 1;
            case "138":
                return 2;
            case "139":
                return 3;
            default:
                return 4;
        }

    }
}
