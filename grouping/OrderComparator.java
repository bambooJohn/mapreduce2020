package com.bambooJohn.mr.grouping;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Description:按照订单编号对数据进行分组
 * @Author: liangzhen
 * @Email: 1033855573@qq.com
 * @Date: 2020-11-19 0:21
 */
public class OrderComparator extends WritableComparator {

    public OrderComparator() {
        super(OrderBean.class,true);
    }

    /**
     * @Description: 分组比较方法，按照相同订单进入一组进行比较
     * @Author: liangzhen
     * @Email: 1033855573@qq.com
     * @create: 2020/11/19 0:25
     * @Param:
     * @return:
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        OrderBean oa = (OrderBean) a;
        OrderBean ob = (OrderBean) b;

        return oa.getOrderId().compareTo(ob.getOrderId());
    }
}
