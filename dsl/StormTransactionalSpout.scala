package storm.scala.dsl

import backtype.storm.topology.base.BaseTransactionalSpout

abstract class StormTransactionalSpout(val outputFields: List[String]) extends BaseTransactionalSpout
                                                              with SetupFunc
                                                              with DeclareOutputFields {}
