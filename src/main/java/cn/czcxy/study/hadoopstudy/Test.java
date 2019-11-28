package cn.czcxy.study.hadoopstudy;

public class Test {
    public static void main(String[] args) {
        String a = "Exception in thread \"main\" java.lang.NullPointerException: a";
        for (String i : a.split(" ")) {
            System.out.println(i.indexOf("Exception:")>0);
            System.out.println(i);
        }
    }
}
