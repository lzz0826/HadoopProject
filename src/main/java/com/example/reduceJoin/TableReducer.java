package com.example.reduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.checkerframework.checker.units.qual.A;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


//<Text, TableBean, TableBean, NullWritable>
//            key使用 TableBean 可以處理 v不用
public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Reducer<Text, TableBean, TableBean, NullWritable>.Context context) throws IOException, InterruptedException {
        //利用 Mapper傳來的格式 key 會join 這裡JOIN 使用pid
        //每種 key會執行一次 reduce 多個會變成(key,TableBean,TableBean) 可以對後面的值做業務處理
        //這裡會出現 兩種 TableBean 表分別處理

        //order有多筆重複
        List<TableBean> orderTs = new ArrayList<>();

        //product每行都單筆
        TableBean productT = new TableBean();

        for (TableBean value : values) {
            String flag = value.getFlag();

            if (flag.equals("order")){
                //創建臨時 TableBean 用 copyProperties addList *避免Hadoop覆蓋(Iterable<TableBean> values 的迭代器跟java不一樣)
                TableBean tmpT = new TableBean();
                try {
                    BeanUtils.copyProperties(tmpT,value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
                orderTs.add(tmpT);
            }else if (flag.equals("product")) {
                try {
                    BeanUtils.copyProperties(productT,value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        //JOIN
        for (TableBean orderT : orderTs) {
            String pid = productT.getPid();
            if (orderT.getPid().equals(pid)){
                orderT.setProductName(productT.getProductName());
            }
            //寫出 k v 如果是obj會使用覆寫的 toString 輸出
            context.write(orderT,NullWritable.get());
        }
    }
}

