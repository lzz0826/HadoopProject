package com.example.mapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;

public class WordCountToolDriver {

    //Tool : 執行下面語句時 為了應對多筆參數 相應處理
    //yarn jar YarnDemo.jar com.example.mapReduce.WordCountTooDriver wordcount /input /output
    private static Tool tool;

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        //拿取 java啟動時的第一個參數 檢驗
        if (args[0].equals("wordcount")) {
            //new 一個地定義的Tool
            tool = new WordCountTool();
        } else {
            throw new RuntimeException("on such tool" + args[0]);
        }

        //取第二個參數到最後一個 給到Tool 裡的
        //FileInputFormat.setInputPaths(job, inputP); //輸入路徑
        //FileOutputFormat.setOutputPath(job,outP); //輸出路徑
        int run = ToolRunner.run(conf, tool, Arrays.copyOfRange(args, 1, args.length));

        System.exit(run);

    }

}
