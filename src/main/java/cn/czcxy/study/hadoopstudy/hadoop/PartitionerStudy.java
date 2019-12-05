package cn.czcxy.study.hadoopstudy.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author weihua
 * @description 结果输出多个文件
 * @启动方法 ./hadoop jar /usr/local/hadoop-study-0.0.1-SNAPSHOT.jar cn.czcxy.study.hadoopstudy.hadoop.PartitionerStudy /input/partitioner.txt /testout4
 * @date 2019/12/2 0002
 **/
public class PartitionerStudy extends BaseStudy{
    public static void main(String[] args) throws Exception {
        /**
         * 输入文件信息
         * hadiip test test hddd @123
         * 123 tex
         * HAD %s test 12
         */
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(getCurrentClass());

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
         * 设置文件切分信息
         * setNumReduceTasks 这个值会传递到PartitionerClass类的方法中
         */
        job.setPartitionerClass(InnerPartitioner.class);
        job.setNumReduceTasks(4);

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
    static class InnerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lineVal = value.toString();
            for (String val : lineVal.split(" ")) {
                context.write(new Text(val), new IntWritable(1));
            }
        }
    }

    /**
     * 内联reducer
     */
    static class InnerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            IntWritable count = new IntWritable(0);
            for (IntWritable i : values) {
                count.set(count.get() + i.get());
            }
            context.write(key, count);
        }
    }

    /**
     * 内联文件分割器
     * 按照数字，大写字母，小写字母，其他 分成四个输出文件
     */
    static class InnerPartitioner extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text text, IntWritable intWritable, int tasks) {
            String val = text.toString();
            if (val.matches("[0-9]*")) {
                return 0 % tasks;
            } else if (val.matches("[a-z]*")) {
                return 1 % tasks;
            } else if (val.matches("[A-Z]*")) {
                return 2 % tasks;
            } else {
                return 3 % tasks;
            }
        }
    }
}
