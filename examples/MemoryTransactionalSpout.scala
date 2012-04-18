package storm.scala.examples;

import backtype.storm.transactional.partitioned.IPartitionedTransactionalSpout
import backtype.storm.topology.OutputFieldsDeclarer
import java.util.Map
import backtype.storm.task.TopologyContext
import backtype.storm.tuple.Fields
import backtype.storm.testing.MemoryTransactionalSpoutMeta


/**
 * This spout only works in local mode.
 */

class MemoryTransactionalSpout(partitions: Map[java.lang.Integer, java.util.List[java.util.List[Object]]], outFields: Fields, takeAmt: Int)
  extends IPartitionedTransactionalSpout[backtype.storm.testing.MemoryTransactionalSpoutMeta] {

  val javaSpout = new backtype.storm.testing.MemoryTransactionalSpout(partitions, outFields, takeAmt)
  val TX_FIELD: String = backtype.storm.testing.MemoryTransactionalSpout.TX_FIELD
  //    public static String TX_FIELD = MemoryTransactionalSpout.class.getName() + "/id";

//    private String _id;
//    private String _finishedPartitionsId;
//    private int _takeAmt;
//    private Fields _outFields;

//    public MemoryTransactionalSpout(Map<Integer, List<List<Object>>> partitions, Fields outFields, int takeAmt) {
//        _id = RegisteredGlobalState.registerState(partitions);
//        Map<Integer, Boolean> finished = Collections.synchronizedMap(new HashMap<Integer, Boolean>());
//        _finishedPartitionsId = RegisteredGlobalState.registerState(finished);
//        _takeAmt = takeAmt;
//        _outFields = outFields;
//    }

  def isExhaustedTuples: Boolean = javaSpout.isExhaustedTuples
//    public boolean isExhaustedTuples() {
//        Map<Integer, Boolean> statuses = getFinishedStatuses();
//        for(Integer partition: getQueues().keySet()) {
//            if(!statuses.containsKey(partition) || !getFinishedStatuses().get(partition)) {
//                return false;
//            }
//        }
//        return true;
//    }

//    class Coordinator implements IPartitionedTransactionalSpout.Coordinator {
//
//        @Override
//        public int numPartitions() {
//            return getQueues().size();
//        }
//
//        @Override
//        public boolean isReady() {
//            return true;
//        }        
//        
//        @Override
//        public void close() {
//        }        
//    }

//    class Emitter implements IPartitionedTransactionalSpout.Emitter<MemoryTransactionalSpoutMeta> {
//        
//        Integer _maxSpoutPending;
//        Map<Integer, Integer> _emptyPartitions = new HashMap<Integer, Integer>();
//        
//        public Emitter(Map conf) {
//            Object c = conf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING);
//            if(c==null) _maxSpoutPending = 1;
//            else _maxSpoutPending = Utils.getInt(c);
//        }
//
//        @Override
//        public MemoryTransactionalSpoutMeta emitPartitionBatchNew(TransactionAttempt tx, BatchOutputCollector collector, int partition, MemoryTransactionalSpoutMeta lastPartitionMeta) {
//            int index;
//            if(lastPartitionMeta==null) {
//                index = 0;
//            } else {
//                index = lastPartitionMeta.index + lastPartitionMeta.amt;
//            }
//            List<List<Object>> queue = getQueues().get(partition);
//            int total = queue.size();
//            int left = total - index;
//            int toTake = Math.min(left, _takeAmt);
//            
//            MemoryTransactionalSpoutMeta ret = new MemoryTransactionalSpoutMeta(index, toTake);
//            emitPartitionBatch(tx, collector, partition, ret);
//            if(toTake==0) {
//                // this is a pretty hacky way to determine when all the partitions have been committed
//                // wait until we've emitted max-spout-pending empty partitions for the partition
//                int curr = Utils.get(_emptyPartitions, partition, 0) + 1;
//                _emptyPartitions.put(partition, curr);
//                if(curr > _maxSpoutPending) {
//                    getFinishedStatuses().put(partition, true);
//                }
//            }
//            return ret;   
//        }
//
//        @Override
//        public void emitPartitionBatch(TransactionAttempt tx, BatchOutputCollector collector, int partition, MemoryTransactionalSpoutMeta partitionMeta) {
//            List<List<Object>> queue = getQueues().get(partition);
//            for(int i=partitionMeta.index; i < partitionMeta.index + partitionMeta.amt; i++) {
//                List<Object> toEmit = new ArrayList<Object>(queue.get(i));
//                toEmit.add(0, tx);
//                collector.emit(toEmit);                
//            }
//        }
//                
//        @Override
//        public void close() {
//        }        
//    } 

  def startup() {javaSpout.startup()}
//    public void startup() {
//        getFinishedStatuses().clear();
//    }

  def cleanup() {javaSpout.cleanup()}
//    public void cleanup() {
//        RegisteredGlobalState.clearState(_id);
//        RegisteredGlobalState.clearState(_finishedPartitionsId);
//    }

//    private Map<Integer, List<List<Object>>> getQueues() {
//        return (Map<Integer, List<List<Object>>>) RegisteredGlobalState.getState(_id);
//    }

//    private Map<Integer, Boolean> getFinishedStatuses() {
//        return (Map<Integer, Boolean>) RegisteredGlobalState.getState(_finishedPartitionsId);
//    }

  def declareOutputFields(declarer: OutputFieldsDeclarer)
    {javaSpout.declareOutputFields(declarer)}
  //    @Override
  //    public void declareOutputFields(OutputFieldsDeclarer declarer) {
  //        List<String> toDeclare = new ArrayList<String>(_outFields.toList());
  //        toDeclare.add(0, TX_FIELD);
  //        declarer.declare(new Fields(toDeclare));
  //    }

  def getComponentConfiguration: Map[String, AnyRef] =
    javaSpout.getComponentConfiguration
  //    @Override
  //    public Map<String, Object> getComponentConfiguration() {
  //        Config conf = new Config();
  //        conf.registerSerialization(MemoryTransactionalSpoutMeta.class);
  //        return conf;
  //    }

  def getCoordinator(conf: Map[_, _], context: TopologyContext): IPartitionedTransactionalSpout.Coordinator =
    javaSpout.getCoordinator(conf, context)
  //    @Override
  //    public IPartitionedTransactionalSpout.Coordinator getCoordinator(Map conf, TopologyContext context) {
  //        return new Coordinator();
  //    }

  def getEmitter(conf: Map[_, _], context: TopologyContext): IPartitionedTransactionalSpout.Emitter[MemoryTransactionalSpoutMeta] =
    javaSpout.getEmitter(conf, context)
  //    @Override
  //    public IPartitionedTransactionalSpout.Emitter<MemoryTransactionalSpoutMeta> getEmitter(Map conf, TopologyContext context) {
  //        return new Emitter(conf);
  //    }
}
