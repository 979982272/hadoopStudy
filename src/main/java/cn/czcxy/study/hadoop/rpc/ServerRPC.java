package cn.czcxy.study.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

/**
 * @author weihua
 * @description
 * @date 2019/11/21 0021
 **/
public class ServerRPC implements ProtocolRPC {
    @Override
    public String testRpc(String test) {
        System.out.println(test);
        return "RPC服务请求成功";
    }

    public static void main(String[] args) throws Exception {
        RPC.Server server = new RPC.Builder(new Configuration())
                .setInstance(new ServerRPC())
                .setBindAddress("127.0.0.1")
                .setPort(8889)
                .setProtocol(ProtocolRPC.class)
                .build();
        server.start();
        System.out.println("rpc服务启动成功...");
    }
}
