package cn.czcxy.study.hadoopstudy.demo01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StudyMain {
    public static void main(String[] args) throws Exception {
        // ./hadoop jar /usr/local/hadoop-study-0.0.1-SNAPSHOT.jar cn.czcxy.study.hadoopstudy.demo01.StudyMain /input/* /testout1
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(StudyMain.class);
        job.setMapperClass(StudyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(StudyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileInputFormat.setInputPaths(job,args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
