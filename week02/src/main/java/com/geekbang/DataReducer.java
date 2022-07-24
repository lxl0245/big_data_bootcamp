package com.geekbang;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataReducer extends Reducer<Text, DataBean, Text, DataBean> {
    @Override
    protected void reduce(Text key, Iterable<DataBean>  v2s, Context context) throws IOException, InterruptedException {
        long up_sum = 0;
        long down_sum = 0;
        for(DataBean bean: v2s) {
            up_sum += bean.getUpPayLoad();
            down_sum += bean.getDownPayLoad();
        }
        DataBean bean = new DataBean("", up_sum, down_sum);
        context.write(key, bean);
    }
}
