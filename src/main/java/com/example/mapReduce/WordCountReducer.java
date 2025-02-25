package com.example.mapReduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//繼承 Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
//     重Mapper來的 輸入KEY 輸入Value , 輸出KEY 輸出Value
//   輸入對應Mapper輸出的類型 輸出對應處理完的類型
// Mapper 輸出的值 等於 Reducer 的出入值
public class WordCountReducer extends Reducer<Text, IntWritable,Text, IntWritable> {


    IntWritable intWritable = new IntWritable();

    //處理 Mapper 傳來的數據 (key,1)
    //每種 key會執行一次 reduce 多個會變成(key,1,1) 可以對後面的值做業務處理
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        int sun = 0;

        for (IntWritable value : values) {
            sun += value.get();
        }

        //intWritable set方法會覆蓋 拉到類 避免循環重複創建浪費資源
        intWritable.set(sun);

        //寫出
        context.write(key,intWritable);


    }
}
