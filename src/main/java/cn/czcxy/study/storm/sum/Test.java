package cn.czcxy.study.storm.sum;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class Test {
    public static void main(String[] args) throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("wcspout", new SumSpout());
        topologyBuilder.setBolt("wcbolt", new SumBolt()).shuffleGrouping("wcspout");
        if (args.length <= 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wc", new Config(), topologyBuilder.createTopology());
        } else {
            StormSubmitter.submitTopology("ws", new Config(), topologyBuilder.createTopology());
        }

    }
}
