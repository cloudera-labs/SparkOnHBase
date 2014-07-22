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

/**
 * A static caching class that will manage all HConnection in a worker
 * 
 * The main idea is there is a hashMap with 
 * HConstants.HBASE_CLIENT_INSTANCE_ID which is ("hbase.client.instance.id")
 * 
 * In that HashMap there is three things
 *   - HConnection
 *   - Number of checked out users of the HConnection
 *   - Time since the HConnection was last used
 *   
 * There is also a Timer thread that will start up every 2 minutes
 * When the Timer thread starts up it will look for HConnection with no
 * checked out users and a last used time that is older then 1 minute.
 * 
 * This class is not intended to be used by Users
 */
object HConnectionStaticCache {
  @transient private val hconnectionMap = 
    new HashMap[String, (HConnection, AtomicInteger, AtomicLong)]

  @transient private val hconnectionTimeout = 60000

  @transient private val hconnectionCleaner = new Timer

  hconnectionCleaner.schedule(new hconnectionCleanerTask, hconnectionTimeout * 2)

  /**
   * Gets or starts a HConnection based on a config object
   */
  def getHConnection(config: Configuration): HConnection = {
    val instanceId = config.get(HConstants.HBASE_CLIENT_INSTANCE_ID)
    var hconnectionAndCounter = hconnectionMap.get(instanceId)
    if (hconnectionAndCounter == null) {
      
      hconnectionMap.synchronized {
        hconnectionAndCounter = hconnectionMap.get(instanceId)
        if (hconnectionAndCounter == null) {
          
          val hConnection = HConnectionManager.createConnection(config)
          hconnectionAndCounter = (hConnection, new AtomicInteger, new AtomicLong)
          hconnectionMap.put(instanceId, hconnectionAndCounter)
        }
      }
    } else {
      //logging
    }
    
    hconnectionAndCounter._2.incrementAndGet()
    return hconnectionAndCounter._1
  }

  /**
   * tell us a thread is no longer using a HConnection
   */
  def finishWithHConnection(config: Configuration, hconnection: HConnection) {
    val instanceId = config.get(HConstants.HBASE_CLIENT_INSTANCE_ID)
    
    var hconnectionAndCounter = hconnectionMap.get(instanceId)
    if (hconnectionAndCounter._2.decrementAndGet() == 0) {
      hconnectionAndCounter._3.set(System.currentTimeMillis())
    }
    
  }

  /**
   * The timer thread that cleans up the HashMap of Collections
   */
  protected class hconnectionCleanerTask extends TimerTask {
    override def run() {
      val it = hconnectionMap.entrySet().iterator()

      val removeList = new MutableList[String]
      
      while (it.hasNext()) {
        val entry = it.next()
        if (entry.getValue()._2.get() == 0 && 
            entry.getValue()._3.get() + 60000 < System.currentTimeMillis()) {
          removeList.+=(entry.getKey())
        }
      }
      
      if (removeList.length > 0) {
        hconnectionMap.synchronized {
          removeList.foreach(key => {
            val v = hconnectionMap.get(key)
            if (v._2.get() == 0 && 
                v._3.get() + 60000 < System.currentTimeMillis()) {
              
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