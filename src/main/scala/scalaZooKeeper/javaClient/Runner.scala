package scalaZooKeeper.javaClient

import org.apache.zookeeper.ZooKeeper
import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.Watcher
import org.apache.zookeeper.Watcher.Event.KeeperState

object Runner {
  
  def main(args: Array[String]){
       val zk = new ZooKeeper("10.45.136.113:2181",5000,new ZK_Watcher())
       
       println(s"ZooKeeper State is ${zk.getState}")

  }
}

class ZK_Watcher extends Watcher{
  
  def process(event: WatchedEvent) {
    println("watcher triggered....")
    println(event.toString())
  }
}