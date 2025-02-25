package com.example.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


//FileOutputFormat<k,v> 接上Reducer 輸出
//自訂輸出方式 過濾 輸出不同地址
// 13560436666 1116 954 2070
// 手機號碼 上行流量 下行流量 總流量

//需求 :
//手機號碼 134 ,135 ,136 開頭 一組 其餘一組 輸出成.log

public class PhoneOutputFormat extends FileOutputFormat<Text, FlowBean> {

    @Override
    public RecordWriter<Text, FlowBean> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {

        //自訂 RecordWriter 需要把 TaskAttemptContext 帶入關聯
        // 13560436666 1116 954 2070
        // 手機號碼 上行流量 下行流量 總流量
        PhoneRecordWriter phoneRecordWriter = new PhoneRecordWriter(job);
        return phoneRecordWriter;
    }
}
