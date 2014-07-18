package spark.hbase

import java.util.HashMap
import org.apache.hadoop.hbase.client.HConnection
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.Timer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.HConnectionManager
import java.util.TimerTask
import scala.collection.mutable.MutableList

object HConnectionStaticCache {
  @transient private val hconnectionMap = new HashMap[String, (HConnection, AtomicInteger, AtomicLong)]

  @transient private val hconnectionTimeout = 60000

  @transient private val hconnectionCleaner = new Timer

  hconnectionCleaner.schedule(new hconnectionCleanerTask, hconnectionTimeout * 2)

  def getHConnection(config: Configuration): HConnection = {
    val instanceId = config.get(HConstants.HBASE_CLIENT_INSTANCE_ID)
    var hconnectionAndCounter = hconnectionMap.get(instanceId)
    if (hconnectionAndCounter == null) {
      println("creating hconnection1: " + instanceId + " " + hconnectionMap.size());
      hconnectionMap.synchronized {
        hconnectionAndCounter = hconnectionMap.get(instanceId)
        if (hconnectionAndCounter == null) {
          
          println("creating hconnection2: " + instanceId);
          
          val hConnection = HConnectionManager.createConnection(config)
          hconnectionAndCounter = (hConnection, new AtomicInteger, new AtomicLong)
          hconnectionMap.put(instanceId, hconnectionAndCounter)
        }
      }
    } else {
      println("reuse hconnection: " + instanceId);
    }
    
    hconnectionAndCounter._2.incrementAndGet()
    return hconnectionAndCounter._1
  }

  def finishWithHConnection(config: Configuration, hconnection: HConnection) {
    val instanceId = config.get(HConstants.HBASE_CLIENT_INSTANCE_ID)
    
    println("finished with hconnection1: " + hconnectionMap.size() );
    
    var hconnectionAndCounter = hconnectionMap.get(instanceId)
    if (hconnectionAndCounter._2.decrementAndGet() == 0) {
      hconnectionAndCounter._3.set(System.currentTimeMillis())
    }
    println("finished with hconnection2: " + hconnectionMap.size() );
  }

  protected class hconnectionCleanerTask extends TimerTask {
    override def run() {
      val it = hconnectionMap.entrySet().iterator()

      val removeList = new MutableList[String]
      
      while (it.hasNext()) {
        val entry = it.next()
        if (entry.getValue()._2.get() == 0 && entry.getValue()._3.get() + 60000 < System.currentTimeMillis()) {
          removeList.+=(entry.getKey())
        }
      }
      
      println("cleaner hconnection: " + removeList.length );
      
      if (removeList.length > 0) {
        hconnectionMap.synchronized {
          removeList.foreach(key => {
            val v = hconnectionMap.get(key)
            if (v._2.get() == 0 && v._3.get() + 60000 < System.currentTimeMillis()) {
              
              println("closing hconnection: " + key);
              
              v._1.close()
              
              hconnectionMap.remove(key);
            }   
          }) 
        }
      }
    }
  }

}