package com.example.elt;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WebLogMapper extends Mapper<LongWritable, Text, Text, NullWritable>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1 獲取1行數據
        String line = value.toString();

        // 2 解析日誌
        boolean result = parseLog(line,context);

        // 3 日誌不合法退出
        if (!result) {
            return;
        }

        // 4 日誌合法就直接寫出
        context.write(value, NullWritable.get());
    }

    // 2 封裝解析日誌的方法
    private boolean parseLog(String line, Context context) {

        // 1 截取
        String[] fields = line.split(" ");

        //可以針對個種正則 清除 if() 沒有加上; true會繼續執行 false 會跳出
//        if (fields[0].matches("正則"))

        // 2 日誌長度大於11的為合法
        if (fields.length > 11) {
            return true;
        }else {
            return false;
        }
    }
}
