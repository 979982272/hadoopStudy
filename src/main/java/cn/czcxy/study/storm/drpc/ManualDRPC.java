package cn.czcxy.study.storm.drpc;

import clojure.lang.Obj;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.DRPCSpout;
import org.apache.storm.drpc.ReturnResults;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


/**
 * 手工模式的drpc
 */
public class ManualDRPC {
    public static class BasicBlot extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            String params = tuple.getString(0);
            // 输出返回的时候，第二个参数必须要获取return-info
            // 详细见org.apache.storm.drpc.execute
            basicOutputCollector.emit(new Values(params + "!!", tuple.getValue(1)));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            // 此为固定格式，第二个参数需要返回return-info
            outputFieldsDeclarer.declare(new Fields("result", "return-info"));
        }
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        LocalDRPC drpc = new LocalDRPC();
        DRPCSpout drpcSpout = new DRPCSpout("callMethod", drpc);
        topologyBuilder.setSpout("reciverSpout", drpcSpout);
        topologyBuilder.setBolt("exeBolt", new BasicBlot(), 3).shuffleGrouping("reciverSpout");
        topologyBuilder.setBolt("returnInfo", new ReturnResults(), 3).shuffleGrouping("exeBolt");
        Config config = new Config();
        if (args.length <= 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("exeBolt", new Config(), topologyBuilder.createTopology());
            System.err.println(drpc.execute("callMethod", "aaa"));
            System.err.println(drpc.execute("callMethod", "aaa"));
            System.exit(0);
        } else {
           // config.put("storm.thrift.transport", "org.apache.storm.security.auth.plain.PlainSaslTransportPlugin");
            StormSubmitter.submitTopology("exeBolt", config, topologyBuilder.createTopology());
        }

    }
}
