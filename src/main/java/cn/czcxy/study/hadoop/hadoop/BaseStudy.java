package cn.czcxy.study.hadoop.hadoop;

/**
 * @author weihua
 * @description
 * @date 2019/12/5 0005
 **/
public class BaseStudy {
    /**
     * 获取当前class
     *
     * @return
     */
    protected static final Class getCurrentClass() {
        return new Object() {
            public Class getClassForStatic() {
                return this.getClass();
            }
        }.getClassForStatic();
    }
}
