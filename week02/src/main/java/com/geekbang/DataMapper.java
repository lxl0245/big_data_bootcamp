package com.geekbang;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataMapper extends Mapper<LongWritable, Text, Text, DataBean> {
    @Override   
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DataBean>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        String telNo = fields[1];
        long up = Long.parseLong(fields[8]);
        long down = Long.parseLong(fields[9]);
        DataBean bean = new DataBean(telNo, up, down);
        context.write(new Text(telNo), bean);
    }
}
