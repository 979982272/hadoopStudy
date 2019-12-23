package cn.czcxy.study.hadoop.hadoop;

import cn.czcxy.study.hadoop.hadfs.HdfsStudy;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author weihua
 * @description mapper 阶段join查询
 * @date 2019/12/2 0002
 **/
public class MappedJoinStudy extends BaseStudy{
    /**
     * 主运行方法,输入文件内容
     * sex
     * 1    男
     * 2    女
     * 3    未知
     * <p>
     * userinfo
     * 1    测试01
     * 2    测试02
     * 3    测试03
     * 4    测试04
     * 5    测试05
     * loginglog
     * id   userid    sex    time
     * 1	1	2	9.27
     * 2	3	1	9.58
     * 3	2	3	9.59
     * 4	5	2	10.00
     * 5	4	1	10.01
     * <p>
     * 启动命令: ./hadoop jar /usr/local/hadoop-study-0.0.1-SNAPSHOT.jar cn.czcxy.study.hadoop.hadoop.MappedJoinStudy /input/loginglog /output/01 /input/sex.txt /input/userinfo.txt
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(getCurrentClass());
        /**
         * 设置mapper 输入的信息
         */
        job.setMapperClass(InnerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        /**
         * 设置文件输入输出信息
         */
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        /**
         * 设置缓存文件
         */
        job.addCacheFile(new URI(args[2]));
        job.addCacheFile(new URI(args[3]));
        job.waitForCompletion(true);
    }

    /**
     * 内联mapper
     */
    static class InnerMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        private Map<String, String> sexMap = new ConcurrentHashMap<>();

        private Map<String, String> nameMap = new ConcurrentHashMap<>();

        /**
         * 开始mapper之前读取缓存文件
         * 设置set和name的缓存
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] uris = context.getCacheFiles();
            String line, fileName, filePath;
            String[] lineInfos;
            BufferedReader bufferedReader = null;
            for (URI uri : uris) {
                filePath = uri.toString();
                InputStream inputStream = HdfsStudy.getFSDataInputStream(filePath).getWrappedStream();
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                bufferedReader = new BufferedReader(inputStreamReader);
                fileName = filePath.substring(filePath.lastIndexOf("/") + 1, filePath.length());
                while ((line = bufferedReader.readLine()) != null) {
                    if (StringUtils.isEmpty(line)) {
                        continue;
                    }
                    lineInfos = line.split("\t");
                    if ("sex.txt".equals(fileName)) {
                        sexMap.put(lineInfos[0], lineInfos[1]);
                    }
                    if ("userinfo.txt".equals(fileName)) {
                        nameMap.put(lineInfos[0], lineInfos[1]);
                    }
                }
            }
        }

        /**
         * 组合拼装结果
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringBuilder userloginlog = new StringBuilder();
            String[] infos = value.toString().split("\t");
            if (nameMap.containsKey(infos[1])) {
                userloginlog.append(infos[0] + "\t" + nameMap.get(infos[1]) + "\t" + sexMap.get(infos[2]) + "\t" + infos[3]);
            }
            context.write(new Text(userloginlog.toString()), NullWritable.get());
        }
    }
}
