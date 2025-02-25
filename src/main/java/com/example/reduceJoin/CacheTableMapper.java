package com.example.reduceJoin;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;


public class CacheTableMapper extends Mapper<LongWritable, Text, Text, NullWritable> {


    //小表 放入緩存 Product key = pid ,  v = productName
    private final Map<String,String> productCache = new HashMap<>();

    private Text out = new Text();


    //初始化方法 處理輸入兩個文件 (每有一個文件都會進一次這方法)
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        //通過緩存文件得到小表數據t_product.txt
        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);

        //獲取文件系統對象,並開流
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream open = fileSystem.open(path);

        //通過包裝流轉換為reader,方便按行讀取
        BufferedReader reader = new BufferedReader(new InputStreamReader(open, StandardCharsets.UTF_8));

        String line;
        while (StringUtils.isNoneEmpty( line = reader.readLine())){
            //03	格力
            String[] split = line.split("\t");
            productCache.put(split[0],split[1]);
        }

        //關流
        IOUtils.closeStream(reader);


    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {

        //取得一行 order
        String str = value.toString();

        String[] split = str.split("\t");

        //1004 	01	4
        String id = split[0];
        String pid = split[1];
        int amount = Integer.parseInt(split[2]);

        //取得對應MAP
        String productName = productCache.get(pid);


        String outStr = id + "\t" + productName + "\t" + amount;

        out.set(outStr);

        context.write(this.out,NullWritable.get());
    }
}
