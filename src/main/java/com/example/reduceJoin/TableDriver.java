package com.example.reduceJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class TableDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        //1、獲取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2、設置jar包路徑
        job.setJarByClass(TableDriver.class);

        //3、關聯mapper和reducer，紐帶為job
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReducer.class);

        // 4 设置Map输出KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        //5、設置最終輸出的kv類型
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);


        //6、設置輸入路徑和輸出路徑
        Path inputP = new Path("/Users/admin/IdeaProjects/HadoopProject/src/main/java/com/example/reduceJoin/input");
        Path outP = new Path("/Users/admin/IdeaProjects/HadoopProject/src/main/java/com/example/reduceJoin/output");

        FileInputFormat.setInputPaths(job, inputP); //输入路径
        FileOutputFormat.setOutputPath(job,outP); //输出路径

        //7、提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
