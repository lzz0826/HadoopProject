package com.example.mapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class WordCountTool implements Tool {


    private Configuration conf ;

    //核心驅動 (conf 區要傳入)
    @Override
    public int run(String[] args) throws Exception {


        Job job = Job.getInstance(this.conf);

        job.setJarByClass(WordCountToolDriver.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path inputP = new Path(args[0]);
        Path outP = new Path(args[1]);

        FileInputFormat.setInputPaths(job, inputP); //輸入路徑
        FileOutputFormat.setOutputPath(job,outP); //輸出路徑

        return  job.waitForCompletion(true) ? 0 : 1;

    }

    @Override
    public void setConf(Configuration conf) {

        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }
}
