package cl.citiaps.storm.rimayprocessor;
import cl.citiaps.storm.rimayprocessor.spouts.*;
import cl.citiaps.storm.rimayprocessor.bolts.*;

import com.google.common.collect.Lists;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.storm.cassandra.bolt.CassandraWriterBolt;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.mongodb.bolt.MongoInsertBolt;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.mongodb.common.mapper.SimpleMongoMapper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.base.BaseWindowedBolt;
import sun.tools.java.Type;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.storm.cassandra.DynamicStatementBuilder.*;

public class RimayProcessorTopology {
    private Properties applicationProperties;

    private RimayProcessorTopology(String applicationProperties) throws IOException {
        this.applicationProperties=new Properties();
        FileInputStream fis=new FileInputStream(applicationProperties);
        this.applicationProperties.load(fis);
        fis.close();
    }
    private void configureBolts(TopologyBuilder builder){
        Map hikariConfigMap=new HashMap<String,String>();
        hikariConfigMap.put("dataSourceClassName","org.mariadb.jdbc.MariaDbDataSource");
        hikariConfigMap.put("dataSource.url",applicationProperties.getProperty("mariadb.url"));
        hikariConfigMap.put("dataSource.user",applicationProperties.getProperty("mariadb.user"));
        hikariConfigMap.put("dataSource.password",applicationProperties.getProperty("mariadb.pass"));
        ConnectionProvider connection= new HikariCPConnectionProvider(hikariConfigMap);

        List<Column> schema= Lists.newArrayList(new Column("id", Types.INTEGER),
                                                new Column("timestamp",Types.BIGINT),
                                                new Column("who",Types.VARCHAR),
                                                new Column("count",Types.BIGINT),
                                                new Column("sum",Types.BIGINT),
                                                new Column("min",Types.INTEGER),
                                                new Column("average",Types.DOUBLE),
                                                new Column("max",Types.INTEGER));

        JdbcMapper mapper=new SimpleJdbcMapper(schema);
        JdbcInsertBolt summaryStatisticsToMariaDBBolt=new JdbcInsertBolt(connection,mapper).withInsertQuery("INSERT INTO statistics (id,timestamp,who,count,sum,min,average,max) VALUES (?,?,?,?,?,?,?,?);");

        CassandraWriterBolt summaryStatisticsToCassandraDBBolt=new CassandraWriterBolt(
                async(
                        boundQuery("INSERT INTO statistics (id,timestamp,who,count,sum,min,average,max) VALUES (?,?,?,?,?,?,?,?);").bind(all())
                )
        );

        MongoMapper mongoMapper=new SimpleMongoMapper().withFields("id","timestamp","who","count","sum","min","average","max");
        MongoInsertBolt summaryStatisticsToMongoDBBolt=new MongoInsertBolt(applicationProperties.getProperty("mongo.url"),applicationProperties.getProperty("mongo.collection"),mongoMapper);

        builder.setBolt("printBolt",new PrintBolt(),1).shuffleGrouping("kafkaSpout");
        builder.setBolt("summaryStatisticsBolt",new SumaryStatisticsBolt().withTumblingWindow(BaseWindowedBolt.Duration.seconds(60)).withTimestampField("timestamp"),1).shuffleGrouping("kafkaSpout");
        builder.setBolt("summaryStatisticsToCassandraDBBolt",summaryStatisticsToCassandraDBBolt).shuffleGrouping("summaryStatisticsBolt");
        builder.setBolt("summaryStatisticsToMariaDBBolt",summaryStatisticsToMariaDBBolt).shuffleGrouping("summaryStatisticsBolt");
        builder.setBolt("summaryStatisticsToMongoDBBolt",summaryStatisticsToMongoDBBolt).shuffleGrouping("summaryStatisticsBolt");
    }

    private void configureSpout(TopologyBuilder builder) {
        KafkaSpout kafkaSpout=new KafkaSpout(this.applicationProperties);
        builder.setSpout("kafkaSpout",kafkaSpout);
    }

    private void buildAndSubmit() throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        TopologyBuilder builder=new TopologyBuilder();
        configureSpout(builder);
        configureBolts(builder);

        Config topologyConfig=new Config();
        Properties kafkaProps=new Properties();
        kafkaProps.put("bootstrap.servers", this.applicationProperties.getProperty("zookeeper.host"));
        kafkaProps.put("acks","1");
        kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        topologyConfig.put("kafka-broker-properties",kafkaProps);
        topologyConfig.put("cassandra.nodes",this.applicationProperties.getProperty("cassandra.nodes"));
        topologyConfig.put("cassandra.port",this.applicationProperties.getProperty("cassandra.port"));
        topologyConfig.put("cassandra.keyspace",this.applicationProperties.getProperty("cassandra.keyspace"));

        StormSubmitter.submitTopology("rimay-processor",topologyConfig,builder.createTopology());
    }

    public static void main (String [ ] args) throws IOException, AlreadyAliveException, InvalidTopologyException, AuthorizationException{
        RimayProcessorTopology topology=new RimayProcessorTopology(args[0]);
        topology.buildAndSubmit();
    }
}
