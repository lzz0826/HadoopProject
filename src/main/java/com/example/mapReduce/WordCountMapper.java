package com.example.mapReduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


//繼承 Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
//   輸入KEY(偏移量)輸入Value , 輸出KEY 輸出Value
//   輸入對應待解析的類型 輸出對應待處理的類型
// Mapper 輸出的值 等於 Reducer 的出入值

public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    private final Text outT = new Text();

    //這裡v給 1 代表這個key(想計算的單詞)出現一次  輸出會是(key,1) 假如多次會有多個 (key,1) 給Reducer處理
    private final IntWritable intWritable =  new IntWritable(1);

    //根據自己輸入的數據分割
    //入數數據每一行 map 方法會被執行一次 針對行李的單詞分割計算
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        //取得一行
        String str = value.toString();

        String[] s = str.split(" ");


        for (String word : s) {
            //Text set方法會覆蓋 拉到類 避免循環重複創建浪費資源
            outT.set(word);
            //寫出 交給Reducer  輸出會是(key,1)
            context.write(outT,intWritable);
        }

    }
}
