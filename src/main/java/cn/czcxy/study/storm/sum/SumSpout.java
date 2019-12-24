package cn.czcxy.study.storm.sum;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;

public class SumSpout extends BaseRichSpout {
    private Map map;
    private TopologyContext topologyContext;
    private SpoutOutputCollector spoutOutputCollector;
    Integer i = 0;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.map = map;
        this.topologyContext = topologyContext;
        this.spoutOutputCollector = spoutOutputCollector;
    }

    /**
     * 发送数据
     */
    @Override
    public void nextTuple() {
        i++;
        Test.logger.info("写入:" + i);
        Values tuple = new Values(i);
        this.spoutOutputCollector.emit(tuple);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("countField"));
    }
}
