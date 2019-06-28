package scalaZooKeeper.curator

import org.apache.curator.framework._
import org.apache.curator.RetryPolicy
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode
import org.apache.curator.framework.recipes.cache.NodeCache
import org.apache.curator.framework.recipes.cache.NodeCacheListener

object Runner {
  val path="/curator"
  
  def main(args: Array[String]){
    
    val retryPolicy =new ExponentialBackoffRetry(1000,3)
    
    val client = CuratorFrameworkFactory.builder()
                                        .connectString("10.45.136.113:2181")
                                        .sessionTimeoutMs(5000)
                                        .retryPolicy(retryPolicy)
                                        .build()
    
    client.start()
    
    // add node
    //client.create().creatingParentsIfNeeded().forPath(path,"test create".getBytes)
    
    
    //2. delete nodes
    //client.delete().deletingChildrenIfNeeded().forPath("/curator")
    
    //3. update data
    //val stat = client.setData().forPath("/curator/help/document","updated data".getBytes)
    
    
    //3. get data
    //val data = client.getData().forPath("/curator/help/document")
    //println(new String(data))
    
    //namingServiceTest(client)
    
    watchTest(client)
  }
  
  def watchTest(client:CuratorFramework){
    //client.create().creatingParentsIfNeeded().forPath(path,"test create".getBytes)
    
    val curData = client.getData.forPath(path)
    println(s"Current data is: ${new String(curData)}")
    
    val cache = new NodeCache(client,path,false)
    cache.start()
    cache.getListenable.addListener(new NodeCacheListener(){
      override def nodeChanged(){
        println(s"Node Changed, new data : ${new String(cache.getCurrentData.getData)}")
      }
    })
    
    client.setData().forPath(path, "new datass".getBytes)
    
    Thread.sleep(Integer.MAX_VALUE)
  }
  
  def namingServiceTest(client: CuratorFramework) = {

    for(i <- 1 to 10 ){
      val thread = new Thread{
        override def run{
          for(j <- 1 to 10){
            val node = client.create()
                             .creatingParentsIfNeeded()
                             .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                             .forPath("/namingServiceTest/job-")
            
            println(s"In thread ${Thread.currentThread().getId}: ${node}")
          }
        }
      }

      thread.start()
    }
  }
}