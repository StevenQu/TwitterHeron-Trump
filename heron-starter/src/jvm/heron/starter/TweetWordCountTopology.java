package heron.starter;

import heron.starter.spout.TwitterSpoutCs4740;
import heron.starter.bolt.IgnoreWordsBolt;
//import heron.starter.bolt.RollingCountBolt;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.ArrayList;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class TweetWordCountTopology {

  private TweetWordCountTopology() { }

  /**
   * A spout that emits a random word
   */
  static class WordSpout extends BaseRichSpout {
    private Random rnd;
    private SpoutOutputCollector collector;
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
      outputFieldsDeclarer.declare(new Fields("word"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext,
                     SpoutOutputCollector spoutOutputCollector) {
      rnd = new Random(31);
      collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
      String[] list = {"Jack", "Mary", "Jill", "McDonald"};
      Utils.sleep(10);
      int nextInt = rnd.nextInt(list.length);
      collector.emit(new Values(list[nextInt]));
    }
  }

  /**
   * A bolt that counts the words that it receives
   */
  static class ConsumerBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, Integer> countMap;
    private int tupleCount;
    private String taskName;
    private long recordTime;
    //StopWordsList
    private Set<String> STOPWORDS = new HashSet<String>(Arrays.asList(new String[]{
 "don't","you're","we're","it's","i'm","let's","rt","a", "as", "able", "about", "above", "according", "accordingly", "across", "actually", "after", "afterwards", "again", "against", "aint", "all", "allow", "allows", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "an", "and", "another", "any", "anybody", "anyhow", "anyone", "anything", "anyway", "anyways", "anywhere", "apart", "appear", "appreciate", "appropriate", "are", "arent", "around", "as", "aside", "ask", "asking", "associated", "at", "available", "away", "awfully", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "believe", "below", "beside", "besides", "best", "better", "between", "beyond", "both", "brief", "but", "by", "cmon", "cs", "came", "can", "cant", "cannot", "cant", "cause", "causes", "certain", "certainly", "changes", "clearly", "co", "com", "come", "comes", "concerning", "consequently", "consider", "considering", "contain", "containing", "contains", "corresponding", "could", "couldnt", "course", "currently", "definitely", "described", "despite", "did", "didnt", "different", "do", "does", "doesnt", "doing", "dont", "done", "down", "downwards", "during", "each", "edu", "eg", "eight", "either", "else", "elsewhere", "enough", "entirely", "especially", "et", "etc", "even", "ever", "every", "everybody", "everyone", "everything", "everywhere", "ex", "exactly", "example", "except", "far", "few", "ff", "fifth", "first", "five", "followed", "following", "follows", "for", "former", "formerly", "forth", "four", "from", "further", "furthermore", "get", "gets", "getting", "given", "gives", "go", "goes", "going", "gone", "got", "gotten", "greetings", "had", "hadnt", "happens", "hardly", "has", "hasnt", "have", "havent", "having", "he", "hes", "hello", "help", "hence", "her", "here", "heres", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "hi", "him", "himself", "his", "hither", "hopefully", "how", "howbeit", "however", "i", "id", "ill", "im", "ive", "ie", "if", "ignored", "immediate", "in", "inasmuch", "inc", "indeed", "indicate", "indicated", "indicates", "inner", "insofar", "instead", "into", "inward", "is", "isnt", "it", "itd", "itll", "its", "its", "itself", "just", "keep", "keeps", "kept", "know", "knows", "known", "last", "lately", "later", "latter", "latterly", "least", "less", "lest", "let", "lets", "like", "liked", "likely", "little", "look", "looking", "looks", "ltd", "mainly", "many", "may", "maybe", "me", "mean", "meanwhile", "merely", "might", "more", "moreover", "most", "mostly", "much", "must", "my", "myself", "name", "namely", "nd", "near", "nearly", "necessary", "need", "needs", "neither", "never", "nevertheless", "new", "next", "nine", "no", "nobody", "non", "none", "noone", "nor", "normally", "not", "nothing", "novel", "now", "nowhere", "obviously", "of", "off", "often", "oh", "ok", "okay", "old", "on", "once", "one", "ones", "only", "onto", "or", "other", "others", "otherwise", "ought", "our", "ours", "ourselves", "out", "outside", "over", "overall", "own", "particular", "particularly", "per", "perhaps", "placed", "please", "plus", "possible", "presumably", "probably", "provides", "que", "quite", "qv", "rather", "rd", "re", "really", "reasonably", "regarding", "regardless", "regards", "relatively", "respectively", "right", "said", "same", "saw", "say", "saying", "says", "second", "secondly", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen", "self", "selves", "sensible", "sent", "serious", "seriously", "seven", "several", "shall", "she", "should", "shouldnt", "since", "six", "so", "some", "somebody", "somehow", "someone", "something", "sometime", "sometimes", "somewhat", "somewhere", "soon", "sorry", "specified", "specify", "specifying", "still", "sub", "such", "sup", "sure", "ts", "take", "taken", "tell", "tends", "th", "than", "thank", "thanks", "thanx", "that", "thats", "thats", "the", "their", "theirs", "them", "themselves", "then", "thence", "there", "theres", "thereafter", "thereby", "therefore", "therein", "theres", "thereupon", "these", "they", "theyd", "theyll", "theyre", "theyve", "think", "third", "this", "thorough", "thoroughly", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "took", "toward", "towards", "tried", "tries", "truly", "try", "trying", "twice", "two", "un", "under", "unfortunately", "unless", "unlikely", "until", "unto", "up", "upon", "us", "use", "used", "useful", "uses", "using", "usually", "value", "various", "very", "via", "viz", "vs", "want", "wants", "was", "wasnt", "way", "we", "wed", "well", "were", "weve", "welcome", "well", "went", "were", "werent", "what", "whats", "whatever", "when", "whence", "whenever", "where", "wheres", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whos", "whoever", "whole", "whom", "whose", "why", "will", "willing", "wish", "with", "within", "without", "wont", "wonder", "would", "would", "wouldnt", "yes", "yet", "you", "youd", "youll", "youre", "youve", "your", "yours", "yourself", "yourselves", "zero"         
 })); 

    //public static final Set<String> STOPWORDS = new HashSet<String>(Arrays.asList(
//	new String[]{"The","the","This","Donald","this"}
//	));   
 public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
      collector = outputCollector;
      countMap = new HashMap<String, Integer>();
      tupleCount = 0;
      taskName = topologyContext.getThisComponentId() + "_" + topologyContext.getThisTaskId();
      recordTime = System.currentTimeMillis();
    }

//Sort Function
public static Map sortByValue(Map unsortedMap){
	Map sortedMap = new TreeMap(new ValueComparator(unsortedMap));
	sortedMap.putAll (unsortedMap);
	return sortedMap;
}
static class ValueComparator implements Comparator {
	Map map;
 
	public ValueComparator(Map map) {
		this.map = map;
	}
 
	public int compare(Object keyA, Object keyB) {
		Comparable valueA = (Comparable) map.get(keyA);
		Comparable valueB = (Comparable) map.get(keyB);
		return valueA.compareTo(valueB);
	}
}
    @Override
    public void execute(Tuple tuple) {
//Declare a new hashmap
      Map<String,Integer> countMap2 = new HashMap<String,Integer>();
      String key = tuple.getString(0).toLowerCase();
      key = key.split(" ")[0];
      tupleCount += 1;
/*      if (tupleCount % 200 == 0) {
        tupleCount = 0;  
      System.out.println(taskName + ":" + Arrays.toString(countMap2.entrySet().toArray()));
	}
*/
//Handle stopwords
     if(!STOPWORDS.contains(key)){
      	if (countMap.get(key) == null) {
        	countMap.put(key, 1);
      	} else {
        	Integer val = countMap.get(key);
        	countMap.put(key, ++val);
      	}
      }
	countMap2 = sortByValue(countMap); 	
	long now = System.currentTimeMillis();
	long diff = (now - recordTime)/1000;
	if (diff > 10) {
        tupleCount = 0; 
//Handle<5 
	ArrayList<String> ret = new ArrayList<String>();
	for (Map.Entry<String,Integer> entry : countMap2.entrySet())
	{
		String key1 = entry.getKey();
		Integer value = entry.getValue();
		if(value > 5)
		ret.add(key1+"="+value);
        }
	countMap.clear();
	recordTime = now;
	System.out.println(taskName + ":" + ret);
	}  
	

    collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
  }

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("word",
    new TwitterSpoutCs4740("dwjMsbQe2RulQiIvxx8ZpjngP",
                 "VHmwucJB5NrMnDjvR8sAqbv71z6YkYbfoCuLGbhTQwlS9Txtxg",
                 "2619468659-1GnSIlSgGEEnDrHPCxyd0L3jEh0Eig9Wg1a3vW1",
                 "rWUM7UrZNAyPeS51suHd9E5irUWvK0q2GMlWTrrlrTjtH"), 1);
    builder.setBolt("count", new ConsumerBolt(), 3).fieldsGrouping("word", new Fields("word"));
//To remove stopwords    
//    builder.setBolt("IgnoreWordsBolt",new IgnoreWordsBolt()).fieldsGrouping("stopwords", new Fields("stopwords"));
    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
   }
    else {
      conf.setMaxTaskParallelism(3);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());

      Thread.sleep(10000);

      cluster.shutdown();
    }
  }
}
