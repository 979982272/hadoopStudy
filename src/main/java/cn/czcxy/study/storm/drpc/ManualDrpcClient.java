package cn.czcxy.study.storm.drpc;

import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;

public class ManualDrpcClient {
    public static void main(String[] args) throws Exception {
        DRPCClient drpcClient = new DRPCClient(Utils.readDefaultConfig(), "aliyun", 3772);
        System.out.println(drpcClient.execute("exc", "1sddd"));
    }
}
