package com.bambooJohn.mr.topN;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Description:
 * @Author: liangzhen
 * @Email: 1033855573@qq.com
 * @Date: 2020-12-07 21:34
 */
public class TopNComparator extends WritableComparator {

    public TopNComparator() {
        super(FlowBean.class,true);
    }

    /**
     * @Description: 将所有数据分到同一组
     * @Author: liangzhen
     * @Email: 1033855573@qq.com
     * @create: 2020/12/7 21:36
     * @Param:
     * @return:
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return 0;
    }
}
