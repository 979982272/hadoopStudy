package cn.czcxy.study.hadoopstudy.hadoop;

import cn.czcxy.study.hadoopstudy.hadfs.HdfsStudy;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author weihua
 * @description mapper 依赖
 * @date 2019/12/2 0002
 **/
public class DependencyStudy extends BaseStudy {
    /**
     * 启动:./hadoop jar /usr/local/hadoop-study-0.0.1-SNAPSHOT.jar cn.czcxy.study.hadoopstudy.hadoop.DependencyStudy /input/info /output/03 /output/04 /input/filter
     * 文件info:
     * A quick grep among the startup scripts reveals that this is
     * part of the hplip service, which provides "HP Linux Imaging and Printi
     * We were heavily influenced by GREP, a popular string-matching utility
     * on UNIX, which had been created in our research center.
     * ne of the best ways to find where the code for a feature you saw in
     * the program resides is to use the grep command
     * <p>
     * 文件filter:
     * A a the is
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job grepJob = Job.getInstance(configuration, "grep_job");
        Job countJob = Job.getInstance(configuration, "count_job");

        grepJob.setJarByClass(getCurrentClass());
        countJob.setJarByClass(getCurrentClass());
        /**
         * 设置grepmapper 输入的信息
         */
        grepJob.setMapperClass(GrepMapper.class);
        grepJob.setMapOutputKeyClass(Text.class);
        grepJob.setMapOutputValueClass(NullWritable.class);
        // 过滤key的文件
        grepJob.addCacheFile(new URI(args[3]));
        FileInputFormat.setInputPaths(grepJob, args[0]);
        FileOutputFormat.setOutputPath(grepJob, new Path(args[1]));

        /**
         * 设置countmapper 输入信息
         */
        countJob.setMapperClass(CountMapper.class);
        countJob.setMapOutputKeyClass(Text.class);
        countJob.setMapOutputValueClass(IntWritable.class);

        /**
         * 设置reduce输出的信息
         */
        countJob.setReducerClass(InnerReducer.class);
        countJob.setOutputKeyClass(Text.class);
        countJob.setOutputValueClass(IntWritable.class);
        /**
         * 设置文件输入输出信息
         */
        FileInputFormat.setInputPaths(countJob, args[1]);
        FileOutputFormat.setOutputPath(countJob, new Path(args[2]));

        /**
         * 设置对应的job依赖
         */
        ControlledJob grepContro = new ControlledJob(grepJob.getConfiguration());
        ControlledJob countContro = new ControlledJob(countJob.getConfiguration());
        countContro.addDependingJob(grepContro);

        /**
         * 设置两个任务的控制
         */
        JobControl jobControl = new JobControl("grep_and_count");
        jobControl.addJob(grepContro);
        jobControl.addJob(countContro);

        /**
         * 启动线程
         */
        System.out.println("开始启动线程");
        Thread thread = new Thread(jobControl);
        thread.start();
        while (!jobControl.allFinished()) {
            Thread.sleep(2000);
        }
        jobControl.stop();
        System.out.println("运行完成");
    }


    /**
     * 过滤mapper
     */
    static class GrepMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        private Map<String, String> filterMap = new ConcurrentHashMap<>();

        /**
         * 从缓存文件中读取需要过滤字段的数据
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] uris = context.getCacheFiles();
            String line, filePath;
            BufferedReader bufferedReader = null;
            for (URI uri : uris) {
                filePath = uri.toString();
                InputStream inputStream = HdfsStudy.getFSDataInputStream(filePath).getWrappedStream();
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                bufferedReader = new BufferedReader(inputStreamReader);
                while ((line = bufferedReader.readLine()) != null) {
                    if (StringUtils.isEmpty(line)) {
                        continue;
                    }
                    for (String key : line.split(" ")) {
                        filterMap.put(key, key);
                    }
                }
            }

        }

        @Override
        protected void map(LongWritable keys, Text info, Context context) throws IOException, InterruptedException {
            for (String val : info.toString().split(" ")) {
                if (filterMap.containsKey(val)) {
                    continue;
                }
                context.write(new Text(val), NullWritable.get());
            }
        }
    }

    /**
     * 计算mapper
     */
    static class CountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable longWritable, Text value, Context context) throws IOException, InterruptedException {
            for (String val : value.toString().split(" ")) {
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
            int count = 0;
            for (IntWritable intWritable : values) {
                count += intWritable.get();
            }
            context.write(new Text(key), new IntWritable(count));
        }
    }
}
