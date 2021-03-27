package com.wxc.mapReduceTest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderReducer extends Reducer<IntWritable, NullWritable, Text, NullWritable> {
    Text k = new Text();
    int i = 0;
    @Override
    protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (final NullWritable value : values) {
            System.out.println("order:"+ i + "\t" + "value: "+key.toString());
            i ++ ;
            String tmp = i + "\t" + key.toString();
            k.set(tmp);

            context.write(k, NullWritable.get());


        }

    }
}
