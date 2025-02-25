package com.example.reduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Map;


public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    private String fileName;

    private Text outT = new Text();

    private TableBean tableBean = new TableBean();


    //初始化方法 處理輸入兩個文件 (每有一個文件都會進一次這方法)
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        //取得一個文件 (每有一個文件都會進一次這方法)
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        this.fileName = fileSplit.getPath().getName();

    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {

        //取得一行 這裡進來的會是不同的表 取到的一行也不同 order product
        String str = value.toString();

        String[] split = str.split("\t");

//        分別處理 order 或 product 有的就設置沒有的給個預設值
        if (fileName.contains("t_order")){
//            id    pid    amout
//            1001  01     1

            //order有的
            tableBean.setId(split[0]);
            tableBean.setPid(split[1]);
            tableBean.setAmount(Integer.parseInt(split[2]));
            //order Flag
            tableBean.setFlag("order");

            //order沒的
            tableBean.setProductName("");


        }else if(fileName.contains("t_product")){
//            pid    pname
//            01     小米

            //product有的
            tableBean.setPid(split[0]);
            tableBean.setProductName(split[1]);
            //product Flag
            tableBean.setFlag("product");

            //product沒的
            tableBean.setId("");
            tableBean.setAmount(0);
        }

        //使用PId當k
        outT.set(tableBean.getPid());

        context.write(outT,tableBean);
    }
}
