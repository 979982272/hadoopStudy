package cn.czcxy.study.hadoop;

import cn.czcxy.study.hadoop.hadoop.BaseStudy;
import org.apache.hadoop.conf.Configuration;

public class Test extends BaseStudy {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Class c = getCurrentClass();
        System.out.println(1);
    }
}
