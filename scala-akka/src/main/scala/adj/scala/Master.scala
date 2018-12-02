package adj.scala

import scala.collection.mutable._

import akka.actor.Actor.Receive
import akka.actor.Actor

import adj.scala.node.WorkerInfo
import adj.scala.message._

import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.actor.Props

/**
 * 脚下留心：
 * 1.参考官网文档
 * 2.scala: 2.12
 * 3.akka: scala=2.12, akka:2.5.x
 */
object Master {
  def main(args: Array[String]): Unit = {
    val host: String = "127.0.0.1"
    val port: Int    = 8081
    
    /**
      * 在实际应用场景下，有时候我们就是确实需要在scala创建多少字符串，但是每一行需要固定对齐。
      * 解决该问题的方法就是应用scala的stripMargin方法，在scala中stripMargin默认是“|”作为出来连接符，
      * 在多行换行的行头前面加一个“|”符号即可。
      * 当然stripMargin方法也可以自己指定“定界符”,同时更有趣的是利用stripMargin.replaceAll方法，
      * 还可以将多行字符串”合并”一行显示。
      */
    val configString = 
      s"""
        |akka.actor.provider="remote"
        |akka.remote.enabled-transports=["akka.remote.netty.tcp"]
        |akka.remote.netty.tcp.hostname="$host"
        |akka.remote.netty.tcp.port="$port" 
      """.stripMargin('|')
    // 通过工厂方法获取一个config对象
    val config = ConfigFactory.parseString(configString)
    
    // 初始化一个ActorSystem
    val actorSystem = ActorSystem("masterActorSystem", config)
    // 使用actorSystem实例化一个名为Master的actor, Worker连接Master时用到masterActor名称
    val actorRef = actorSystem.actorOf(Props(new Master(host, port)), "masterActor")
    
    println(s"First: $actorRef")
  }
}

class Master(val host:String, val port: Int) extends Actor {
  /** checkout worker interval */
  val checkoutWorkerInterval: Int = 5000
  /** worker info relate to worker id*/
  val workerInfos = new HashMap[String, WorkerInfo]()
  
  /** execute the function after master run */
  override def preStart(): Unit = {
    // import time unit
    import scala.concurrent.duration._
    // import implicit convert
    import context.dispatcher
    
    // schedule to send message for self
    context.system.scheduler.schedule(0 millis, checkoutWorkerInterval millis, self, CheckoutWorker)
  }
  
  override def receive: Receive = {
    case "started" => {
      println("master already started")
    }
    // receive message for register worker from worker
    case RegisterWorker (workerId, cores, memory) => {
      println("receive worker register message, worker info: " + workerId + " " + cores + " " + memory)
      
      val worker = WorkerInfo(workerId, cores, memory)
      // cache
      workerInfos(workerId) = worker
      sender() ! RegisteredWorker(host, port)
    }
    case HeartBeat(workerId) => {
      println("receive worker heart message, worker info: " + workerId)
      
      val workerInfo  = workerInfos(workerId)
      workerInfo.recentHeartBeatTime = System.currentTimeMillis()
      workerInfos(workerId) = workerInfo
    }
    case CheckoutWorker => {
      val currentTime = System.currentTimeMillis()
      val deadWorkers = workerInfos.filter(w => ((currentTime - w._2.recentHeartBeatTime) > checkoutWorkerInterval * 2))
      for(w <- deadWorkers) {
        workerInfos -= w._1
      }
      
      println("checkout worker state, live: " + workerInfos.size)
    }
  }
}