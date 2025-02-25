package com.example.mapReduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class WordCountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        //1、獲取job
        Configuration conf = new Configuration();

//        // 開啟 map 端輸出壓縮 查詢可用壓縮方式到集群: hadoop checknative
//        conf.setBoolean("mapreduce.map.output.compress", true);
//       // 設置 map 端輸出壓縮方式 影響傳輸速度 不會改片最後輸出的文件
//        conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);

        Job job = Job.getInstance(conf);

        //2、設置jar包路徑
        job.setJarByClass(WordCountDriver.class);

        //3、關聯mapper和reducer，紐帶為job
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //4、設置map輸出的kv類型(反射)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5、設置最終輸出的kv類型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        //CombineTextInputFormat : 針對小文件 如果不設置默認使用 TextInputFormat.class
        job.setInputFormatClass(CombineTextInputFormat.class);

        //虛擬存儲切片最大值4m 設定大可以減少分片數 相當於減少任務數 解決大量小文件 小文件合併處理
        CombineTextInputFormat.setMaxInputSplitSize(job,4194304);



        //6、設置輸入路徑和輸出路徑
        Path inputP = new Path("/Users/admin/IdeaProjects/HadoopProject/src/main/java/com/example/mapReduce/HelloWord.txt");
        Path outP = new Path("/Users/admin/IdeaProjects/HadoopProject/src/main/java/com/example/mapReduce/test");

        // *使用啟動時帶進來的動態參數 可以指定想分析的數據 打包後上傳Hadoop集群
        // 執行指令 :  hadoop jar xxx.jar com.example.mapReduce.WordCountDriver /input /output
//        Path inputP = new Path(args[0]);
//        Path outP = new Path(args[1]);
        FileInputFormat.setInputPaths(job, inputP); //輸入路徑
        FileOutputFormat.setOutputPath(job,outP); //輸出路徑


//        // 設置 reduce 端輸出壓縮開啟 查詢可用壓縮方式到集群: hadoop checknative
//        FileOutputFormat.setCompressOutput(job, true);
//        // 設置壓縮的方式
//        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
////         FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
////         FileOutputFormat.setOutputCompressorClass(job,DefaultCodec.class);


        //7、提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
