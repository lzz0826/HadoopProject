package com.example.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

//自定義分區策略 在Mapper輸出之後 <k,v>等於map輸出

//手機號136/137/138/139開頭的分別放進4個獨立的文件里，然後其他的手機號放到一個文件里。最終形成5個文件。
public class PhonePartitioner extends Partitioner<Text, FlowBean> {

//    當NumReduceTask > getPartition()里定義的分區數量，可以正常運行，但是相應的，會多餘生成一些空的文件，浪費計算資源和存儲資源；
//    當 1 < NumReduceTask < getPartition()分區量，會報IO異常，因為少的那一部分分區的數據會無法寫入；
//    當NumReduceTask = 1時，不會調用自定義分區器，而是會將所有的數據都交付給一個ReduceTask，最後也只會生成一個文件。
//    自定義分區類時，分區號必須從0開始，且必須是連續的，即是逐一累加的。

    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {

        //用map傳來的key(手機號分析)
        String phone = text.toString();

        //取前三位 0 1 2 站位
        String substring = phone.substring(0,3);

        //返回的是分區 需要在Driver中配置對應 分區數
        switch(substring){
            case "136" :
                return 0;
            case "137" :
                return 1;
            case "138" :
                return 2;
            case "139" :
                return 3;
            default:
                //其他分區
                return 4;
        }
    }


}
