package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class MainMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
    private IntWritable revenueWritable = new IntWritable(0);
    private Text laptopText = new Text();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException
    {
        String line = value.toString();

        System.out.println("Ini adalah daftar laptop");
        System.out.println(line);

        String[] split = line.split("-");
        String laptop = split[0];
        laptop = laptop.trim(); //Hilangkan spasi di depan/belakang

        String[] pendapatanSplit = split[1].split(" ");

        String pendapatan = pendapatanSplit[(pendapatanSplit.length-1)];

        this.laptopText.set(laptop);
        this.revenueWritable.set(Integer.parseInt(pendapatan));
        output.collect(this.laptopText, this.revenueWritable);
    }
}
