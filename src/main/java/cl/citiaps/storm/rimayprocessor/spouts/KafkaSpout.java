package cl.citiaps.storm.rimayprocessor.spouts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.tuple.Values;

public class KafkaSpout implements IRichSpout {
    private Consumer<String,String> consumer;
    private Properties applicationProperties;
    private ObjectMapper mapper;

    private SpoutOutputCollector collector;
    private TopologyContext context;

    public KafkaSpout(Properties applicationProperties){
        this.applicationProperties=applicationProperties;
    }

    @Override
    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
        this.context=context;
        this.collector=collector;

        Properties kafkaConfig=new Properties();
        kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,applicationProperties.getProperty("kafka.broker.list"));
        kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG,applicationProperties.getProperty("kafka.consumer.group"));
        kafkaConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        kafkaConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        this.consumer=new KafkaConsumer<>(kafkaConfig);
        this.consumer.subscribe(Collections.singletonList(applicationProperties.getProperty("kafka.rimay.raw.topic")));

        this.mapper=new ObjectMapper();
    }

    @Override
    public void nextTuple() {
        ConsumerRecords<String,String> records=this.consumer.poll(Duration.ofMillis(10));

        for(ConsumerRecord<String,String> record : records){
            try {
                JsonNode json=this.mapper.readTree(record.value());
                this.collector.emit(new Values(json.get("who").asText(),json.get("where").asText(),json.get("how-many").asInt(),json.get("what").asText(),json.get("timestamp").asLong()));
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("who","where","how-many","what","timestamp"));
    }

    @Override
    public void activate(){

    }

    @Override
    public void deactivate(){

    }

    @Override
    public void close(){

    }

    @Override
    public void ack(Object object){

    }
    @Override
    public void fail(Object object){

    }

    @Override
    public Map<String, Object> getComponentConfiguration(){
        return(null);
    }
}
