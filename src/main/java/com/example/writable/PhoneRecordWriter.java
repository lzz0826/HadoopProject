package com.example.writable;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;


//需求 :
//手機號碼 134 ,135 ,136 開頭 一組 其餘一組 輸出成.log
// 13560436666 1116 954 2070
// 手機號碼 上行流量 下行流量 總流量
public class PhoneRecordWriter extends RecordWriter<Text, FlowBean> {

    private FSDataOutputStream filterOut;
    private FSDataOutputStream otherOut;


    //構造把 TaskAttemptContext 帶進來
    public PhoneRecordWriter(TaskAttemptContext job) {

        try {

            //獲取文件系統對象
            FileSystem fs = FileSystem.get(job.getConfiguration());
            //用文件系統對象創建兩個輸出流對應不同的目錄
            Path filterOutP = new Path("/Users/admin/IdeaProjects/HadoopProject/src/main/java/com/example/writable/test3/filter.log");
            Path otherOutP = new Path("/Users/admin/IdeaProjects/HadoopProject/src/main/java/com/example/writable/test3/other.log");
            this.filterOut = fs.create(filterOutP);
            this.otherOut = fs.create(otherOutP);
        }catch (IOException e){
            System.out.println(e.getMessage());

        }
    }


    //輸出流
    @Override
    public void write(Text key, FlowBean value) throws IOException, InterruptedException {

        // 13560436666 1116 954 2070
        // 手機號碼 上行流量 下行流量 總流量
        String phone = key.toString().substring(0, 3);

        //手機號碼 134 ,135 ,136 開頭 一組 其餘一組 輸出成.log
        if(phone.equals("134") || phone.equals("135") || phone.equals("136")){
            this.filterOut.write((phone + value).getBytes(StandardCharsets.UTF_8));
            this.filterOut.write("\n".getBytes());
        }else {
            this.otherOut.write((phone + value).getBytes(StandardCharsets.UTF_8));
            this.otherOut.write("\n".getBytes());
        }
    }


    //關閉資源
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        //關閉數據流
        filterOut.close();
        otherOut.close();

    }
}
