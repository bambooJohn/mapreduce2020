package com.bambooJohn.mr.grouping;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description:
 * @Author: liangzhen
 * @Email: 1033855573@qq.com
 * @Date: 2020-11-18 23:44
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private String orderId;
    private String productId;
    private Double price;

    public OrderBean() {
    }

    public OrderBean(String orderId, String productId, Double price) {
        this.orderId = orderId;
        this.productId = productId;
        this.price = price;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return orderId + "\t" + productId + "\t" + price;
    }

    /**
     * @Description: 排序逻辑：先按照订单排序，订单相同按照价格降序排列
     * @Author: liangzhen
     * @Email: 1033855573@qq.com
     * @create: 2020/11/18 23:55
     * @Param:
     * @return:
     */
    @Override
    public int compareTo(OrderBean o) {
        int compare = this.orderId.compareTo(o.orderId);
        if(compare != 0){
            return compare;
        }else{
            return Double.compare(o.price,this.price);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.orderId);
        out.writeUTF(this.productId);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.productId = in.readUTF();
        this.price = in.readDouble();
    }
}
