package heron.starter;

import java.util.LinkedList;

import backtype.storm.Config;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.NotAliveException;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import heron.starter.bolt.IntermediateRankingsBolt;
import heron.starter.bolt.RollingCountBolt;
import heron.starter.bolt.TotalRankingsBolt;
import heron.starter.tools.RankableObjectWithFields;
import heron.starter.tools.Rankings;
import heron.starter.util.StormRunner;

/**
 * This topology does a continuous computation of the top N words that the topology has seen in terms of cardinality.
 * The top N computation is done in a completely scalable way, and a similar approach could be used to compute things
 * like trending topics or trending images on Twitter.
 */
public class RollingTopWords {

  private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;
  private static final int TOP_N = 5;

  private final TopologyBuilder builder;
  private final String topologyName;
  private final Config topologyConfig;
  private final int runtimeInSeconds;

  public RollingTopWords() throws InterruptedException {
    builder = new TopologyBuilder();
    topologyName = "slidingWindowCounts";
    topologyConfig = createTopologyConfiguration();
    runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

    wireTopology();
  }

  private static Config createTopologyConfiguration() {
    Config conf = new Config();
    conf.setDebug(true);
    conf.registerSerialization(Rankings.class);
    conf.registerSerialization(RankableObjectWithFields.class);
    conf.registerSerialization(LinkedList.class);
    return conf;
  }

  private void wireTopology() throws InterruptedException {
    String spoutId = "wordGenerator";
    String counterId = "counter";
    String intermediateRankerId = "intermediateRanker";
    String totalRankerId = "finalRanker";
    builder.setSpout(spoutId, new TestWordSpout(), 5);
    builder.setBolt(counterId, new RollingCountBolt(9, 3), 4).fieldsGrouping(spoutId, new Fields("word"));
    builder.setBolt(intermediateRankerId, new IntermediateRankingsBolt(TOP_N), 4).fieldsGrouping(counterId, new Fields(
        "obj"));
    builder.setBolt(totalRankerId, new TotalRankingsBolt(TOP_N)).globalGrouping(intermediateRankerId);
  }

  public void run() throws InterruptedException, InvalidTopologyException, AlreadyAliveException, NotAliveException {
    StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
  }

  public static void main(String[] args) throws Exception {
    new RollingTopWords().run();
  }
}
