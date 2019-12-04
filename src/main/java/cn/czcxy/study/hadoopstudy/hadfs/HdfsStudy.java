package cn.czcxy.study.hadoopstudy.hadfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;

/**
 * @author weihua
 * @description
 * @date 2019/11/19 0019
 **/
public class HdfsStudy {

    public static void main(String[] args) throws IOException {
        readHdfsFile("/input/core-site.xml");
        // copyToLocalFile("/input/core-site.xml", "C://core-site.xml");
        // copyFromLocalFile("C://ADJUSTORDER.txt", "/input");
    }

    /**
     * 上传本地文件到hdfs
     *
     * @param src
     * @param dst
     * @throws IOException
     */
    private static void copyFromLocalFile(String src, String dst) throws IOException {
        FileSystem fileSystem = getFileSystem();
        fileSystem.copyFromLocalFile(new Path(src), new Path(dst));
    }

    /**
     * 复制hdfs文件到本地
     *
     * @param src
     * @param dst
     * @throws IOException
     */
    private static void copyToLocalFile(String src, String dst) throws IOException {
        FileSystem fileSystem = getFileSystem();
        fileSystem.copyToLocalFile(new Path(src), new Path(dst));
    }

    /**
     * 读取hdfs文件
     *
     * @param path
     */
    private static void readHdfsFile(String path) throws IOException {
        FSDataInputStream fsDataInputStream = getFSDataInputStream(path);
        IOUtils.copyBytes(fsDataInputStream, System.out, 4096, true);
    }

    /**
     * 获取hdfs的文件流
     *
     * @param path
     * @return
     * @throws IOException
     */
    public static FSDataInputStream getFSDataInputStream(String path) throws IOException {
        FileSystem fileSystem = getFileSystem();
        return fileSystem.open(new Path(path));
    }

    /**
     * 获取文件系统
     *
     * @return
     * @throws IOException
     */
    private static FileSystem getFileSystem() throws IOException {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        FileSystem fileSystem = FileSystem.get(getConfiguration());
        return fileSystem;
    }

    /**
     * 获取配置
     *
     * @return
     */
    public static Configuration getConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://47.93.187.183:9000");
        configuration.set("hadoop.home.dir", "/usr/local/hadoop-3.1.2");
        return configuration;
    }
}
