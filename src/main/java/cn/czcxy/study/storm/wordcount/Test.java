package cn.czcxy.study.storm.wordcount;

import cn.czcxy.study.storm.sum.SumBolt;
import cn.czcxy.study.storm.sum.SumSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class Test {

    public static void main(String[] args) throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("wcspout", new WcSpout());
        // 讲拿到的数据切分后分发给下一个bolt
        topologyBuilder.setBolt("wcsplitbolt", new WcSplitBolt(), 3).shuffleGrouping("wcspout");
        // 配置多线程，指定通过属性分发
        topologyBuilder.setBolt("wcbolt", new WcBolt(), 3).fieldsGrouping("wcsplitbolt", new Fields("word"));
        if (args.length <= 0) {
            // 启动为本地集群
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordcount", new Config(), topologyBuilder.createTopology());
        } else {
            // 启动为远程集群
            StormSubmitter.submitTopology("wordcount", new Config(), topologyBuilder.createTopology());
        }

    }
}
