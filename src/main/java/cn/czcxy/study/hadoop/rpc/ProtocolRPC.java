package cn.czcxy.study.hadoop.rpc;

import org.apache.hadoop.ipc.ProtocolInfo;

/**
 * @author weihua
 * @description
 * @date 2019/11/21 0021
 **/
@ProtocolInfo(protocolName = "testRPC", protocolVersion = 1L)
public interface ProtocolRPC {
    String testRpc(String test);
}
