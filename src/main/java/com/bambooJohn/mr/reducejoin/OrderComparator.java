package com.bambooJohn.mr.reducejoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Description:分组比较器，按照Order对象的Pid分组
 * @Author: liangzhen
 * @Email: 1033855573@qq.com
 * @Date: 2020-11-25 11:40
 */
public class OrderComparator extends WritableComparator {

    public OrderComparator() {

        super(OrderBean.class,true);

    }

    /**
     * @Description: 按照PID比较a和b
     * @Author: liangzhen
     * @Email: 1033855573@qq.com
     * @create: 2020/11/25 13:39
     * @Param:
     * @return:
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        OrderBean oa = (OrderBean) a;
        OrderBean ob = (OrderBean) b;

        return oa.getPid().compareTo(ob.getPid());

    }
}
