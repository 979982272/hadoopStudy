package cn.czcxy.study.storm.drpc;

import org.apache.storm.Config;
import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;

public class ManualDrpcClient {
    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.put("storm.thrift.transport", "org.apache.storm.security.auth.plain.PlainSaslTransportPlugin");
        config.put("storm.nimbus.retry.times", 1);
        config.put("storm.nimbus.retry.interval.millis", 1);
        config.put("storm.nimbus.retry.intervalceiling.millis", 1);
        DRPCClient drpcClient = new DRPCClient(Utils.readDefaultConfig(), "aliyun", 3772);
        System.out.println(drpcClient.execute("exc", "1sddd"));
    }
}
