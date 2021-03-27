package com.wxc.mapReduceTest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {
    @Override
    protected void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {

        // 1.读取一行文本转换为Int
        final int num = Integer.parseInt(value.toString());
        // 2.将num作为key的输出，Map输出<num , Null>
        context.write( new IntWritable(num), NullWritable.get());

    }
}
