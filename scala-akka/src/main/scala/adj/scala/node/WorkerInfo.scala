package adj.scala.node

/** worker basic infomation */
case class WorkerInfo(var workerId: String, var cores: Int, var memory: Int) {
  /** worker recent heart beat time */
  var recentHeartBeatTime: Long = System.currentTimeMillis()
}