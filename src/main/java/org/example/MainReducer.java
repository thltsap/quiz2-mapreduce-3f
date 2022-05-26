package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class MainReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{
    @Override
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
                       Reporter reporter) throws  IOException
    {
        System.out.println("-----------------");
        System.out.println("Data Laptop Harga Terendah dan Tertinggi");
        System.out.println("Key: " + key.toString());
        System.out.println("-----------------");

        int min = Integer.MAX_VALUE;
        int max = 0;

        while (values.hasNext())
        {
            IntWritable currentRevenue = values.next();
            System.out.println("Data Laptop Terbaru: " + currentRevenue.get());

            if (currentRevenue.get() < min)
            {
                min = currentRevenue.get();
            }
            if (currentRevenue.get() > max)
            {
                max = currentRevenue.get();
            }
        }
        output.collect(key, new IntWritable(min));
        output.collect(key, new IntWritable(max));
    }
}
