/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package storm.starter;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.KafkaConfig.StaticHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.starter.bolt.PrinterBolt;

public class KafkaTopology {
    
    public static void main(String[] args) throws Exception {
    	
    	List<String> hosts = new ArrayList<String>();
        hosts.add("localhost");
        SpoutConfig kafkaConf = new SpoutConfig(StaticHosts.fromHostString(hosts, 3), "test", "/kafkastorm", "discovery");
        kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConf);
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", kafkaSpout, 2);
        builder.setBolt("printer", new PrinterBolt())
                .shuffleGrouping("spout");
        
        Config config = new Config();
        config.setDebug(true);
        
        if(args!=null && args.length > 0) {
            config.setNumWorkers(3);
            
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } else {        
            config.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("kafka", config, builder.createTopology());
        
            Thread.sleep(10000);

            cluster.shutdown();
        }
       
    }
    
}
