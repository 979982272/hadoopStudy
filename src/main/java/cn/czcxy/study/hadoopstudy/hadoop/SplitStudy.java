package cn.czcxy.study.hadoopstudy.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 启动方法
 * ./hadoop jar /usr/local/hadoop-study-0.0.1-SNAPSHOT.jar cn.czcxy.study.hadoopstudy.hadoop.SplitStudy /input/* /testout1
 */
public class SplitStudy {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(SplitStudy.class);
        /**
         * 设置mapper 输入的信息
         */
        job.setMapperClass(InnerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        /**
         * 设置reduce输出的信息
         */
        job.setReducerClass(InnerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        /**
         * 设置文件输入输出信息
         */
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

    }


    /**
     * 内联mapper
     */
    class InnerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
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

    /**
     * 内联reducer
     */
    class InnerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            IntWritable count = new IntWritable(0);
            for (IntWritable i : values) {
                count.set(count.get() + i.get());
            }
            context.write(key, count);
        }
    }

}




