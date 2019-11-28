package cn.czcxy.study.hadoopstudy.demo01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


import java.io.IOException;

public class StudyMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String lineVal = value.toString();
        for (String val : lineVal.split(" ")) {
            if (val.indexOf("Exception:") > 0) {
                context.write(new Text(val), new IntWritable(1));
            }

        }
    }
}
