package com.example.writable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


//需求 : 統計每一個手機號耗費的總上行流量、下行流量、總流量

//輸入數據格式：7 13560436666 120.196.100.99 1116 954 200
//            id 手機號碼 網絡ip 上行流量 下行流量 網絡狀態碼

//期望輸出數據格式
// 13560436666 1116 954 2070
// 手機號碼 上行流量 下行流量 總流量

//實現 Writable 因資源可能被拆分到不同data節點 在節點互相通訊時需要 序列化 反序列化
public class FlowBean implements Writable {

    //上行流量
    private long upFlow;

    //下行流量
    private long downFlow;

    //總流量
    private long sumFlow;

    //聲明空餐構造 給反射用
    public FlowBean() {
    }

    //序列化 順序必須跟反序列化一致
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.upFlow);
        out.writeLong(this.downFlow);
        out.writeLong(this.sumFlow);
    }

    //反序列化 順序必須跟序列化一致
    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    //總和方法
    public void setSumFlow(){
        this.sumFlow = this.upFlow + this.downFlow;
    }


    // 期望輸出數據格式
    // 13560436666 1116 954 2070
    // 手機號碼 上行流量 下行流量 總流量
    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
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
}
