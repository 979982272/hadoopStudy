package cn.czcxy.study.storm.drpc.linear;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * @author weihua
 * @description
 * @date 2019/12/24 0024
 **/
public class LinearDrpcServer {
    public static class LinearBlot extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            String input = tuple.getString(1);
            // 参数格式为固定格式
            basicOutputCollector.emit(new Values(tuple.getValue(0), input + "!"));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            // 此为固定格式
            outputFieldsDeclarer.declare(new Fields("id", "result"));
        }
    }

    public static void main(String[] args) throws Exception {
        // 调用方法的名字
        String function = "callMethod";
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder(function);
        builder.addBolt(new LinearBlot(), 3);
        Config conf = new Config();
        if (args.length <= 0) {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("drpc-demo", conf, builder.createLocalTopology(drpc));
            for (String word : new String[]{"hello", "goodbye"}) {
                System.err.println("Result for \"" + word + "\": " + drpc.execute(function, word));
            }
            System.exit(0);
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createRemoteTopology());
        }
    }
}
