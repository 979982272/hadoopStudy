package cn.czcxy.study.storm.sum;

import cn.czcxy.study.LogTestUtil;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public class Test {
    public static Logger logger = LoggerFactory.getLogger(Test.class);

    public static void main(String[] args) throws Exception {
        LogTestUtil.write("c://test.log","sss");
//        TopologyBuilder topologyBuilder = new TopologyBuilder();
//        topologyBuilder.setSpout("wcspout", new SumSpout());
//        topologyBuilder.setBolt("wcbolt", new SumBolt()).shuffleGrouping("wcspout");
//        logger.info("-------------" + args.length);
//        if (args.length <= 0) {
//            LocalCluster cluster = new LocalCluster();
//            cluster.submitTopology("wc", new Config(), topologyBuilder.createTopology());
//        } else {
//            StormSubmitter.submitTopology(args[0], new Config(), topologyBuilder.createTopology());
//        }

    }
}
