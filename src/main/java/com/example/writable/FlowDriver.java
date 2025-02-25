package com.example.writable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class FlowDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        //1、獲取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2、設置jar包路徑
        job.setJarByClass(FlowDriver.class);

        //3、關聯mapper和reducer，紐帶為job
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //4、設置map輸出的kv類型(反射)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //5、設置最終輸出的kv類型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //設置自定義 (分區)
//        job.setPartitionerClass(PhonePartitioner.class);
        //ReduceTask的数量：區要根據 ProvincePartitioner 自定義的分區開數量
        //少於自定義的分區開數量會報IO異常 多會產生空分區浪費資源
//        job.setNumReduceTasks(5);

        //使用自定義的 (輸出流) 過濾 手機號碼 134 ,135 ,136 開頭 一組 其餘一組 輸出成.log
//        job.setOutputFormatClass(PhoneOutputFormat.class);

        //6、設置輸入路徑和輸出路徑
        Path inputP = new Path("/Users/admin/IdeaProjects/HadoopProject/src/main/java/com/example/writable/test99.txt");
        Path outP = new Path("/Users/admin/IdeaProjects/HadoopProject/src/main/java/com/example/writable/test3");

        // *使用啟動時帶進來的動態參數 可以指定想分析的數據 打包後上傳Hadoop集群
        // 執行指令 :  hadoop jar xxx.jar com.example.mapReduce.WordCountDriver /input /output
//        Path inputP = new Path(args[0]);
//        Path outP = new Path(args[1]);
        FileInputFormat.setInputPaths(job, inputP); //输入路径
        FileOutputFormat.setOutputPath(job,outP); //输出路径

        //7、提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
