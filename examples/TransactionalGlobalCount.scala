package storm.scala.examples

//import storm.scala.dsl._
import backtype.storm.Config
import backtype.storm.LocalCluster
import backtype.storm.testing.MemoryTransactionalSpout
import backtype.storm.tuple.{Fields, Tuple, Values}
import collection.JavaConversions._
import collection.mutable.{Map, HashMap}
import java.math.BigInteger
import backtype.storm.topology.base.{BaseTransactionalBolt, BaseBatchBolt}
import backtype.storm.coordination.BatchOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.transactional.{TransactionAttempt, ICommitter, TransactionalTopologyBuilder}
import java.util.Map

object TransactionalGlobalCount {
  val PARTITION_TAKE_PER_BATCH = 3
  val DATA = new HashMap[java.lang.Integer, java.util.List[java.util.List[AnyRef]]]()
  DATA += ( (0, List[java.util.List[AnyRef]](List(new Values("cat"),
                                                  new Values("dog"),
                                                  new Values("chicken"),
                                                  new Values("cat"),
                                                  new Values("dog"),
                                                  new Values("apple"))))
    ,       (1, List[java.util.List[AnyRef]](List(new Values("cat"),
                                                  new Values("dog"),
                                                  new Values("apple"),
                                                  new Values("banana"))))
    ,       (2, List[java.util.List[AnyRef]](List(new Values("cat"),
                                                  new Values("cat"),
                                                  new Values("cat"),
                                                  new Values("cat"),
                                                  new Values("cat"),
                                                  new Values("dog"),
                                                  new Values("dog"),
                                                  new Values("dog"),
                                                  new Values("dog"))))
    )

  class Value {
    var count: java.lang.Integer = 0
    var txid: BigInteger = BigInteger.ZERO
  }

  val DATABASE = new java.util.HashMap[String, Value]()

  val GLOBAL_COUNT_KEY = "GLOBAL-COUNT"

//  public static Map<String, Value> DATABASE = new HashMap<String, Value>();
//  public static final String GLOBAL_COUNT_KEY = "GLOBAL-COUNT";

  class BatchCount extends BaseBatchBolt {
    var _id: Object = null.asInstanceOf[Object]
    var _collector: BatchOutputCollector = null
    var _count: java.lang.Integer = 0

    def prepare(conf: java.util.Map[_,_], context: TopologyContext, collector: BatchOutputCollector, id: Nothing) {
      _collector = collector
      _id = id
    }

    override def execute(tuple: Tuple) {
      _count += 1
    }

    override def finishBatch() {
      _collector.emit(new Values(_id, _count))
    }

    override def declareOutputFields(declarer: OutputFieldsDeclarer) {
      declarer declare(new Fields("id", "count"))
    }

  }

  //  public static class BatchCount extends BaseBatchBolt {
//    Object _id;
//    BatchOutputCollector _collector;
//
//    int _count = 0;
//
//    @Override
//    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
//      _collector = collector;
//      _id = id;
//    }
//
//    @Override
//    public void execute(Tuple tuple) {
//      _count++;
//    }
//
//    @Override
//    public void finishBatch() {
//      _collector.emit(new Values(_id, _count));
//    }
//
//    @Override
//    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//      declarer.declare(new Fields("id", "count"));
//    }
//  }

  class UpdateGlobalCount extends BaseTransactionalBolt with ICommitter {
    var _attempt: TransactionAttempt = null
    var _collector: BatchOutputCollector = null
    var _sum: java.lang.Integer = 0

    def prepare(conf: java.util.Map[_,_], context: TopologyContext, collector: BatchOutputCollector, attempt: TransactionAttempt) {
      _collector = collector
      _attempt = attempt
    }

    override def execute(tuple: Tuple) {
      _sum += tuple.getInteger(1)
    }

    override def finishBatch() {
      val value: Value = DATABASE.get(GLOBAL_COUNT_KEY)
      var newValue = new Value
      if (value == null || !value.txid.equals(_attempt.getTransactionId)) {
        newValue = new Value
        newValue.txid = _attempt.getTransactionId
        if (value == null) {
          newValue.count = _sum
        } else {
          newValue.count = _sum + value.count
        }
        DATABASE.put(GLOBAL_COUNT_KEY, newValue)
      } else {
        newValue = value
      }
      _collector.emit(new Values(_attempt, newValue.count));
    }

    override def declareOutputFields(declarer: OutputFieldsDeclarer) {
      declarer declare(new Fields("id", "sum"))
    }
  }

//  public static class UpdateGlobalCount extends BaseTransactionalBolt implements ICommitter {
//    TransactionAttempt _attempt;
//    BatchOutputCollector _collector;
//
//    int _sum = 0;
//
//    @Override
//    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt attempt) {
//      _collector = collector;
//      _attempt = attempt;
//    }
//
//    @Override
//    public void execute(Tuple tuple) {
//      _sum+=tuple.getInteger(1);
//    }
//
//    @Override
//    public void finishBatch() {
//      Value val = DATABASE.get(GLOBAL_COUNT_KEY);
//      Value newval;
//      if(val == null || !val.txid.equals(_attempt.getTransactionId())) {
//        newval = new Value();
//        newval.txid = _attempt.getTransactionId();
//        if(val==null) {
//          newval.count = _sum;
//        } else {
//          newval.count = _sum + val.count;
//        }
//        DATABASE.put(GLOBAL_COUNT_KEY, newval);
//      } else {
//        newval = val;
//      }
//      _collector.emit(new Values(_attempt, newval.count));
//    }
//
//    @Override
//    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//      declarer.declare(new Fields("id", "sum"));
//    }
//  }

  def main(args: Array[String])  {
    val spout   = new MemoryTransactionalSpout(DATA, new Fields("word"), PARTITION_TAKE_PER_BATCH)
    val builder = new TransactionalTopologyBuilder("global-count", "spout", spout, 3)

    builder.setBolt("partial-count", new BatchCount, 5)
        .noneGrouping("spout")
    builder.setBolt("sum", new UpdateGlobalCount)
        .globalGrouping("partial-count")

    val cluster = new LocalCluster

    val config = new Config
    config.setDebug(true)
    config.setMaxSpoutPending(3)

    cluster.submitTopology("global-count-topology", config, builder.buildTopology)

    Thread sleep 3000
    cluster.shutdown()
  }
}
