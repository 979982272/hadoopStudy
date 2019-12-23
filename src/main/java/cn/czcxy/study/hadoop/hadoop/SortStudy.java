package cn.czcxy.study.hadoop.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @author weihua
 * @description 排序
 * @date 2019/12/2 0002
 **/
public class SortStudy extends BaseStudy{
    /**
     * 输入数据
     * 1	35
     * 1	32
     * 1	59
     * 2	31
     * 1	68
     * 3	1
     * 2	56
     * 3	15
     * 1	56
     * 4	8
     * 9	7
     * 启动命令: ./hadoop jar /usr/local/hadoop-study-0.0.1-SNAPSHOT.jar cn.czcxy.study.hadoop.hadoop.SortStudy /input/sort /output/01
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
        job.setMapOutputKeyClass(SortWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        /**
         * 设置reduce输出的信息
         */
        job.setReducerClass(InnerReducer.class);
        job.setOutputKeyClass(SortWritable.class);
        job.setOutputValueClass(NullWritable.class);
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
    static class InnerMapper extends Mapper<LongWritable, Text, SortWritable, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] valInfos = value.toString().split("\t");
            context.write(new SortWritable(Integer.parseInt(valInfos[0]), Integer.parseInt(valInfos[1])), NullWritable.get());
        }
    }

    /**
     * 内联reducer
     */
    static class InnerReducer extends Reducer<SortWritable, NullWritable, SortWritable, NullWritable> {
        @Override
        protected void reduce(SortWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    static class SortWritable implements WritableComparable<SortWritable> {
        private int first;
        private int second;

        @Override
        public int compareTo(SortWritable o) {
            int temp = this.first - o.first;
            if (temp != 0) {
                System.out.println(temp);
                return temp;
            }
            System.out.println(this.second - o.second);
            return this.second - o.second;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(this.first);
            dataOutput.writeInt(this.second);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.first = dataInput.readInt();
            this.second = dataInput.readInt();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SortWritable that = (SortWritable) o;
            return first == that.first &&
                    second == that.second;
        }

        @Override
        public int hashCode() {
            return Objects.hash(first, second);
        }

        @Override
        public String toString() {
            return "SortWritable{" +
                    "first=" + first +
                    ", second=" + second +
                    '}';
        }

        public SortWritable(int first, int second) {
            this.first = first;
            this.second = second;
        }

        public SortWritable() {
        }

        public int getFirst() {
            return first;
        }

        public void setFirst(int first) {
            this.first = first;
        }

        public int getSecond() {
            return second;
        }

        public void setSecond(int second) {
            this.second = second;
        }
    }
}
