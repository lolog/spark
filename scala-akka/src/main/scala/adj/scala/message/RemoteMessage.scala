package adj.scala.message

trait RemoteMessage extends Serializable {
  
}

/** register worker for master */
case class RegisterWorker(var workerId: String, var cores: Int, var memory: Int) extends RemoteMessage

/** registered worker response for worker */
case class RegisteredWorker(val host:String, val port: Int) extends RemoteMessage

/** master receive heart beat*/
case class HeartBeat(val workerId: String)

/** master send heart beat response for worker */
case object WorkerHeartBeat

/** checkout worker schedule*/
case object CheckoutWorker