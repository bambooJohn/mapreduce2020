package com.bambooJohn.mr.compare;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Description:
 * @Author: liangzhen
 * @Email: 1033855573@qq.com
 * @Date: 2020-11-15 22:53
 */
public class FlowComparator extends WritableComparator {

    protected FlowComparator() {
        super(FlowBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        FlowBean fa = (FlowBean) a;
        FlowBean fb = (FlowBean) b;

        return Long.compare(fb.getSumFlow(),fa.getSumFlow());

    }
}
