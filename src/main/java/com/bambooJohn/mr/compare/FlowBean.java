package com.bambooJohn.mr.compare;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description:
 * @Author: liangzhen
 * @Email: 1033855573@qq.com
 * @Date: 2020-11-05 18:54
 */
public class FlowBean implements WritableComparable<FlowBean> {

    private long upFlow;
    private long downFlow;
    private long sumFlow;

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    public void set(long upFlow,long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    /**
     * @Description: 将对象数据写出到框架指定地方
     * @Author: liangzhen
     * @Email: 1033855573@qq.com
     * @create: 2020/11/5 19:04
     * @Param:out 数据的容器
     * @return:
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    /**
     * @Description:从框架指定地方读取数据填充对象
     * @Author: liangzhen
     * @Email: 1033855573@qq.com
     * @create: 2020/11/5 19:05
     * @Param: in 数据的容器
     * @return:
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    /**
     * @Description: 比较方法，按照总流量降序排序
     * @Author: liangzhen
     * @Email: 1033855573@qq.com
     * @create: 2020/11/13 20:47
     * @Param:
     * @return:
     */
    @Override
    public int compareTo(FlowBean o) {

        /*if(this.sumFlow < o.sumFlow){
            return -1;
        }else if(this.sumFlow == o.sumFlow){
            return 0;
        }else{
            return 1;
        }*/

        return Long.compare(o.sumFlow,this.sumFlow);

    }
}
