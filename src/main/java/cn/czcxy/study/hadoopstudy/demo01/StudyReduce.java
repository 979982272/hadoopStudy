package cn.czcxy.study.hadoopstudy.demo01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class StudyReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        IntWritable count = new IntWritable(0);
        for (IntWritable i : values) {
            count.set(count.get() + i.get());
        }
        context.write(key, count);
    }
}
