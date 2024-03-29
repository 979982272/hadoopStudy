package cn.czcxy.study.storm.drpc.manual;

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
 * 手工模式的drpc 作废
 */
@Deprecated
public class ManualDrpcServer {

    public static class ManualBlot extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
//             输出返回的时候，第二个参数必须要获取return-info
//             详细见org.apache.storm.drpc.execute
            String params = tuple.getString(0);
            basicOutputCollector.emit(new Values(params + "!!", tuple.getValue(1)));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            // 此为固定格式，第二个参数需要返回return-info
            outputFieldsDeclarer.declare(new Fields("result", "return-info"));
        }
    }


    public static void main(String[] args) throws Exception {
       /* TopologyBuilder topologyBuilder = new TopologyBuilder();
        LocalDRPC drpc = new LocalDRPC();
        DRPCSpout drpcSpout = new DRPCSpout("exclamation", drpc);
        topologyBuilder.setSpout("reciverSpout", drpcSpout);
        topologyBuilder.setBolt("exeBolt", new ManualBlot(), 3).shuffleGrouping("reciverSpout");
        topologyBuilder.setBolt("returnInfo", new ReturnResults(), 3).shuffleGrouping("exeBolt");
        Config config = new Config();
        if (args.length <= 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("exeBolt", new Config(), topologyBuilder.createTopology());
            System.err.println(drpc.execute("exclamation", "aaa"));
            System.err.println(drpc.execute("exclamation", "aaa"));
            System.exit(0);
        } else {
            config.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
        }*/
        TopologyBuilder builder = new TopologyBuilder();
        LocalDRPC drpc = new LocalDRPC();

        DRPCSpout spout = new DRPCSpout("exclamation", drpc);
        builder.setSpout("drpc", spout);
        builder.setBolt("exclaim", new ManualBlot(), 3).noneGrouping("drpc");
        builder.setBolt("return", new ReturnResults(), 3).noneGrouping("exclaim");
        Config conf = new Config();
        if (args.length <= 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("exclaim", conf, builder.createTopology());
            System.out.println(drpc.execute("exclamation", "aaa"));
            System.out.println(drpc.execute("exclamation", "bbb"));
            System.exit(0);
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }

    }
}
