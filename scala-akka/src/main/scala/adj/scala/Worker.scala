package adj.scala

import java.util.UUID

import adj.scala.message._

import akka.actor.Actor
import akka.actor.ActorSelection
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.actor.Props

object Worker {
  def main(args: Array[String]): Unit = {
    val workerHost: String = "localhost"
    val workerPort: Int    = 8082
    
    val masterHost: String = "localhost"
    val masterPort: Int    = 8081
    
    val configString = 
      s"""
        |akka.actor.provider="remote"
        |akka.remote.enabled-transports=["akka.remote.netty.tcp"]
        |akka.remote.netty.tcp.hostname="$workerHost"
        |akka.remote.netty.tcp.port="$workerPort" 
      """.stripMargin('|')
    val config = ConfigFactory.parseString(configString)
    // create worker actor
    val workerActorSystem = ActorSystem("workerActorSystem", config)
    
    // create worker actor
    val workerActor =  workerActorSystem.actorOf(Props(new Worker(masterHost, masterPort, 62, 128)), "workerActor")
    
    // await
    workerActorSystem.whenTerminated
  }
}

class Worker(val masterHost: String, val masterPort: Int, var cores: Int, var memory: Int) extends Actor {
  /** master actor by akka-url */
  var masterActor: ActorSelection = _
  /** worker id */
  val workerId: String = UUID.randomUUID().toString()
  
   val heartBeatInterval: Int = 3000
  
  override def preStart(): Unit = {
    val akkaUrl: String = s"akka.tcp://masterActorSystem@127.0.0.1:8081/user/masterActor"
    masterActor = context.actorSelection(akkaUrl)
    
    // send message for master
    masterActor ! RegisterWorker(workerId, cores, memory)
  }
  
  override def receive: Receive = {
    case RegisteredWorker(host, port) => {
      println("receive master registered message, master host:" + host + ":" + port)
      
      // send heart beat for master after registered worker successful 
      import scala.concurrent.duration._
      import context.dispatcher
      
      context.system.scheduler.schedule(0 millis, heartBeatInterval millis, self, WorkerHeartBeat)
    }
    case WorkerHeartBeat => {
      masterActor ! HeartBeat(workerId)
    }
  }
}