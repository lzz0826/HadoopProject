package com.example.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//繼承 Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
//     重Mapper來的 輸入KEY 輸入Value , 輸出KEY 輸出Value
//   輸入對應Mapper輸出的類型 輸出對應處理完的類型
// Mapper 輸出的值 等於 Reducer 的出入值

//需求 : 統計每一個手機號耗費的總上行流量、下行流量、總流量

//輸入數據格式：7 13560436666 120.196.100.99 1116 954 200
//            id 手機號碼 網絡ip 上行流量 下行流量 網絡狀態碼

//期望輸出數據格式
// 13560436666 1116 954 2070
// 手機號碼 上行流量 下行流量 總流量
public class FlowReducer extends Reducer<Text, FlowBean,Text, FlowBean> {

    private final FlowBean outFlowBean = new FlowBean();


    //處理 Mapper 傳來的數據 (key,1)
    //每種 key會執行一次 reduce 多個會變成(key,1,1) 可以對後面的值做業務處理
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {


        long totalUpFlow = 0;
        long totalDownFlow = 0;

        //在處理每行 如果有同樣手機號在相加
        for (FlowBean value : values) {
            totalUpFlow += value.getUpFlow();
            totalDownFlow += value.getDownFlow();
        }

        this.outFlowBean.setUpFlow(totalUpFlow);
        this.outFlowBean.setDownFlow(totalDownFlow);
        this.outFlowBean.setSumFlow();

        //寫出 key phone , v = 手機號碼 上行流量 下行流量 總流量
        context.write(key,this.outFlowBean);

    }
}
