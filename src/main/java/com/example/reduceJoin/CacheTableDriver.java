package com.example.reduceJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class CacheTableDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {

        //1、獲取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2、設置jar包路徑
        job.setJarByClass(CacheTableDriver.class);

        //3、關聯mapper 只有用到Mapper做JOIN ，紐帶為job
        job.setMapperClass(CacheTableMapper.class);

        // 4 設置Map輸出KV類型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        //5、設置最終輸出的kv類型
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 加載緩存數據
        job.addCacheFile(new URI("file:///Users/admin/IdeaProjects/HadoopProject/src/main/java/com/example/reduceJoin/input/t_product"));
        // Map端Join的邏輯不需要Reduce階段，設置reduceTask數量為0
        job.setNumReduceTasks(0);


        //6、設置輸入路徑和輸出路徑
        Path inputP = new Path("/Users/admin/IdeaProjects/HadoopProject/src/main/java/com/example/reduceJoin/input/t_order.txt");
        Path outP = new Path("/Users/admin/IdeaProjects/HadoopProject/src/main/java/com/example/reduceJoin/output");

        FileInputFormat.setInputPaths(job, inputP); //輸入路徑
        FileOutputFormat.setOutputPath(job,outP); //輸出路徑

        //7、提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
