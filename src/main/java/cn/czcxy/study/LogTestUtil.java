package cn.czcxy.study;

import cn.czcxy.study.utils.RedisUtil;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

/**
 * @author weihua
 * @description
 * @date 2019/12/24 0024
 **/
public class LogTestUtil {
    public static void write(String path, String content) {
        File file = new File(path);
        try {
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fileWriter = new FileWriter(file.getAbsoluteFile(), true);
            PrintWriter pw = new PrintWriter(fileWriter);
            pw.println(content);
            pw.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {

        }
    }
}
