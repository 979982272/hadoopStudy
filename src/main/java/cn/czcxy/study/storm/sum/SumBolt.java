package cn.czcxy.study.storm.sum;

import cn.czcxy.study.LogTestUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class SumBolt extends BaseRichBolt {
    int count = 0;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {
        count = count + tuple.getIntegerByField("countField");
        Test.logger.info("相加:" + count);
        LogTestUtil.write("/test.log","相加:" + count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
