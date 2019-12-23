package cn.czcxy.study.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.net.InetSocketAddress;

/**
 * @author weihua
 * @description
 * @date 2019/11/21 0021
 **/
public class ClientRPC {
    public static void main(String[] args) throws Exception {
        ProtocolRPC rpc = RPC.getProxy(ProtocolRPC.class, 1L, new InetSocketAddress("127.0.0.1", 8889), new Configuration());
        String res = rpc.testRpc("测试请求rpc服务");
        System.out.println(res);
    }
}
