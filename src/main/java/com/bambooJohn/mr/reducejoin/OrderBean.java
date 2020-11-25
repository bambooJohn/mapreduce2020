package com.bambooJohn.mr.reducejoin;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description:
 * @Author: liangzhen
 * @Email: 1033855573@qq.com
 * @Date: 2020-11-25 11:07
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private String id;
    private String pid;
    private String pname;
    private Integer amount;

    public OrderBean() {
    }

    public OrderBean(String id, String pid, String pname, Integer amount) {
        this.id = id;
        this.pid = pid;
        this.pname = pname;
        this.amount = amount;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public Integer getAmount() {
        return amount;
    }

    public void setAmount(Integer amount) {
        this.amount = amount;
    }

    @Override
    public String toString() {
        return this.id + "\t" + this.pname + "\t" + this.amount;
    }

    /**
     * @Description:
     * 按照pid分组，最内按照Pname降序排列
     * @Author: liangzhen
     * @Email: 1033855573@qq.com
     * @create: 2020/11/25 11:39
     * @Param:
     * @return:
     */
    @Override
    public int compareTo(OrderBean o) {
        int compare = this.pid.compareTo(o.getPid());
        if(compare != 0){
            return compare;
        }else {
            return o.getPname().compareTo(this.pname);
        }

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.id);
        out.writeUTF(this.pid);
        out.writeUTF(this.pname);
        out.writeInt(this.amount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.pid = in.readUTF();
        this.pname = in.readUTF();
        this.amount = in.readInt();
    }
}
