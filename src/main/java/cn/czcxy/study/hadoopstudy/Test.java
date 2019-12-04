package cn.czcxy.study.hadoopstudy;

import com.sun.tools.hat.internal.parser.ReadBuffer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.WildcardType;
import java.net.URI;

public class Test {
    public static void main(String[] args) throws Exception {
        InputStream inputStream = new FileInputStream("c://sex.txt");
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {

            System.out.println(line);
        }
    }
}
