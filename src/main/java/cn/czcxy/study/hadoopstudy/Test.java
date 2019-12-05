package cn.czcxy.study.hadoopstudy;

import cn.czcxy.study.hadoopstudy.hadoop.BaseStudy;
import com.sun.tools.hat.internal.parser.ReadBuffer;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.WildcardType;
import java.net.URI;

public class Test extends BaseStudy {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Class c = getCurrentClass();
        System.out.println(1);
    }
}
