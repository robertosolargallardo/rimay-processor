package cl.citiaps.storm.rimayprocessor.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summarizingInt;

public class SumaryStatisticsBolt extends BaseWindowedBolt {
    private OutputCollector collector;
    private static final AtomicInteger id=new AtomicInteger(0);

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
    }
    @Override
    public void execute(TupleWindow wtuple) {
        Timestamp timestamp=new Timestamp(System.currentTimeMillis());
        List<Tuple> tuples=wtuple.get();
        tuples.sort(Comparator.comparing(this::getTimestamp));

        Map<String,IntSummaryStatistics> result=tuples.stream().collect(groupingBy(tuple -> tuple.getStringByField("who"),TreeMap::new,summarizingInt(tuple -> tuple.getIntegerByField("how-many"))));
        for (Map.Entry<String,IntSummaryStatistics> entry : result.entrySet())
        {
            this.collector.emit(new Values(id.getAndIncrement(),timestamp.getTime(),entry.getKey(),entry.getValue().getCount(),entry.getValue().getSum(),entry.getValue().getMin(),entry.getValue().getAverage(),entry.getValue().getMax()));
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id","timestamp","who","count","sum","min","average","max"));
    }
    private Long getTimestamp(Tuple tuple) {
        return tuple.getLongByField("timestamp");
    }
}
