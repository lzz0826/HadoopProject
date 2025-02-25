package com.example.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;


//繼承 Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
//   輸入KEY(偏移量)輸入Value , 輸出KEY 輸出Value
//   輸入對應待解析的類型 輸出對應待處理的類型
// Mapper 輸出的值 等於 Reducer 的出入值

//需求 : 統計每一個手機號耗費的總上行流量、下行流量、總流量
//輸入數據格式：7 13560436666 120.196.100.99 1116 954 200
//            id 手機號碼 網絡ip 上行流量 下行流量 網絡狀態碼
//期望輸出數據格式
// 13560436666 1116 954 2070
// 手機號碼 上行流量 下行流量 總流量

public class FlowMapper extends Mapper<LongWritable, Text,Text, FlowBean> {

    private final Text outT = new Text();

    private final FlowBean flowBean = new FlowBean();


    //根據自己輸入的數據分割
    //入數數據每一行 map 方法會被執行一次 針對行李的單詞分割計算
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {

        //取得一行
        String str = value.toString();

        //輸入數據格式：7 13560436666     120.196.100.99                   1116     954        200
        //            1	13736230513 	192.196.100.1	www.baidu.com	 2481	  24681	     200
        //           id 手機號碼           網絡ip            網址          上行流量   下行流量  網絡狀態碼
        String[] s = str.split("\t");

        System.out.println(Arrays.toString(s));

        String phone = s[1];

        //重後取避免網址有些空
        String upFlow = s[s.length - 3];
        String downFlow = s[s.length - 2];

        flowBean.setUpFlow(Long.parseLong(upFlow));
        flowBean.setDownFlow(Long.parseLong(downFlow));
        flowBean.setSumFlow();

        outT.set(phone);
        context.write(outT,flowBean);

    }
}
