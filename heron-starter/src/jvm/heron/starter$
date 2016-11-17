package heron.wordcount.Topologies;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import heron.wordcount.bolts.WordCounter;
import heron.wordcount.bolts.WordNormalizer;
import heron.wordcount.spouts.WordReader;

/**
 * Created by Felix on 15/11/23.
 */
public class TopologyMain {
    public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException {
        //定义拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReader(),5);
        builder.setBolt("word-normalizer", new WordNormalizer(),5).shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounter(),5).shuffleGrouping("word-normalizer");


        //配置
        Config conf = new Config();
//        conf.setNumWorkers(3);
//        conf.setNumAckers(5);

        //运行拓扑
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 10);
        StormSubmitter.submitTopology("Getting-Started-Topologie", conf, builder.createTopology());


    }
}
