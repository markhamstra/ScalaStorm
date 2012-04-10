package storm.scala.dsl

import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.tuple.Fields
import collection.JavaConversions._

trait DeclareOutputFields {
  val outputFields: List[String]

  def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(new Fields(outputFields))
  }
}
