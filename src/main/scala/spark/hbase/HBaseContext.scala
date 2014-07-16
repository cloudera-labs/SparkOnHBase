package spark.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.rdd.RDD
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.io.ByteArrayInputStream
import java.io.DataInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.spark.api.java.JavaPairRDD
import java.io.OutputStream
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Get
import java.util.ArrayList
import org.apache.hadoop.hbase.client.Result
import scala.reflect.ClassTag
import org.apache.hadoop.hbase.client.HConnection
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.client.Delete

@serializable class HBaseContext(conf: Configuration) {
  
  def this() = this(null)  
  
  var configBytes: Array[Byte] = Option(conf).map { c =>
    val baos = new ByteArrayOutputStream()
    c.write(new DataOutputStream(baos))
    val array = baos.toByteArray()
    baos.close()
    array
  }.orNull
  
  
  def foreach[T](rdd: RDD[T], f:(Iterator[T], HConnection) => Unit) = {
	  rdd.foreachPartition(it => hbaseForeachFunction(configBytes, it, f))
  }
  
  def bulkPuts[T](rdd: RDD[T], tableName: String, f:(T) => Put, autoFlush: Boolean) {
    rdd.foreachPartition(
        it => hbaseForeachFunction[T] (
            configBytes,
            it, 
            (iterator, hConnection) => {
              var htable = hConnection.getTable(tableName)
              htable.setAutoFlush(autoFlush, true)
              iterator.foreach(T => htable.put(f(T)))
              htable.close()
            }))
  }
  
  def bulkIncrements[T](rdd: RDD[T], tableName: String, f:(T) => Increment, autoFlush: Boolean) {
    rdd.foreachPartition(
        it => hbaseForeachFunction[T] (
            configBytes,
            it, 
            (iterator, hConnection) => {
              var htable = hConnection.getTable(tableName)
              htable.setAutoFlush(autoFlush, true)
              iterator.foreach(T => htable.increment(f(T)))
              htable.close()
            }))
  }
  
  def bulkDeletes[T](rdd: RDD[T], tableName: String, f:(T) => Delete, autoFlush: Boolean) {
    rdd.foreachPartition(
        it => hbaseForeachFunction[T] (
            configBytes,
            it, 
            (iterator, hConnection) => {
              var htable = hConnection.getTable(tableName)
              htable.setAutoFlush(autoFlush, true)
              iterator.foreach(T => htable.delete(f(T)))
              htable.close()
            }))
  }
  
  
  def bulkGets[K, U: ClassTag](tableName: String,
      batchSize:Integer,
      rdd: RDD[K],
      makeGet:(K) => Get,
      convertResult:(Result) => U): RDD[U] = {
    rdd.mapPartitions[U]( 
        it => hbaseBatchGetMapIterator[K, U](configBytes, 
        tableName, 
        batchSize, 
        it, 
        makeGet, 
        convertResult), true)
  }
  
  private def hbaseForeachFunction[T](hbaseConf: Array[Byte],
      it : Iterator[T], 
      f:(Iterator[T], HConnection) => Unit) = {
    
    val config = deserializeConfig(hbaseConf)
	  
	  var hConnection = HConnectionManager.createConnection(config)
	  
	  f(it, hConnection)
	  
    hConnection.close();
  }
  
  
  private def hbaseBatchGetMapIterator[K, U](hbaseConf: Array[Byte],
      tableName: String,
      batchSize: Integer,
      it : Iterator[K], 
      makeGet:(K) => Get,
      convertResult:(Result) => U): Iterator[U] = {
    
    val config = deserializeConfig(hbaseConf)
    
    var hConnection = HConnectionManager.createConnection(config)
    var htable = hConnection.getTable(tableName)
    var gets = new ArrayList[Get]();
    var res = List[U]()
    
    while (it.hasNext) {
      gets.add(makeGet(it.next))
      
      if (gets.size() == batchSize) {
        var results = htable.get(gets)
        res = res ++ results.map(convertResult)
        gets.clear()
      }
    }
    if (gets.size() > 0) {
      var results = htable.get(gets)
      res = res ++ results.map(convertResult)
      gets.clear()
    }
    hConnection.close();
    res.iterator
  }
  
  private def deserializeConfig(hbaseConf: Array[Byte]) : Configuration = {
    val bais = new ByteArrayInputStream(hbaseConf);
    val in = new DataInputStream(bais);
    val config = new Configuration();
    config.readFields(in);
    bais.close();
    config
  }
}