package cl.citiaps.storm.rimayprocessor.bolts;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;


import java.util.Map;

public class PrintBolt implements IRichBolt{
    private Map config;
    private TopologyContext context;
    private OutputCollector collector;

    @Override
    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        this.config=config;
        this.context=context;
        this.collector=collector;
    }

    @Override
    public void execute(Tuple input) {
		System.out.println(input.getStringByField("who")+" "+
                           input.getStringByField("where")+" "+
                           input.getStringByField("what")+" "+
                           input.getIntegerByField("how-many")+" "+
                           input.getLongByField("timestamp"));
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
