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
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.mapreduce.TableMapper
import org.apache.hadoop.hbase.mapreduce.IdentityTableMapper
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.mapreduce.MutationSerialization
import org.apache.hadoop.hbase.mapreduce.ResultSerialization
import org.apache.hadoop.hbase.mapreduce.KeyValueSerialization
import org.apache.spark.rdd.HadoopRDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SerializableWritable
import java.util.HashMap
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos
import java.util.concurrent.atomic.AtomicInteger
import org.apache.hadoop.hbase.HConstants
import java.util.concurrent.atomic.AtomicLong
import java.util.Timer
import java.util.TimerTask
import org.apache.hadoop.hbase.client.Mutation
import scala.collection.mutable.MutableList

@serializable class HBaseContext(@transient sc: SparkContext, broadcastedConf: Broadcast[SerializableWritable[Configuration]]) {
  
  
  
  def this(@transient sc: SparkContext, @transient config: Configuration) {
    this(sc, sc.broadcast(new SerializableWritable(config)))
  }

  def foreachPartition[T](rdd: RDD[T], f: (Iterator[T], HConnection) => Unit) = {
    rdd.foreachPartition(it => hbaseForeachPartition(broadcastedConf, it, f))
  } 

  def mapPartition[T, U: ClassTag](rdd: RDD[T],
    mp: (Iterator[T], HConnection) => Iterator[U]): RDD[U] = {

    rdd.mapPartitions[U](it => hbaseMapPartition[T, U](broadcastedConf,
      it,
      mp), true)
  }

  def bulkPut[T](rdd: RDD[T], tableName: String, f: (T) => Put, autoFlush: Boolean) {

    rdd.foreachPartition(
      it => hbaseForeachPartition[T](
        broadcastedConf,
        it,
        (iterator, hConnection) => {
          val htable = hConnection.getTable(tableName)
          htable.setAutoFlush(autoFlush, true)
          iterator.foreach(T => htable.put(f(T)))
          htable.close()
        }))
  }

  def bulkMutation[T](rdd: RDD[T], tableName: String, f: (T) => Mutation, batchSize: Integer) {
    rdd.foreachPartition(
      it => hbaseForeachPartition[T](
        broadcastedConf,
        it,
        (iterator, hConnection) => {
          val htable = hConnection.getTable(tableName)
          val mutationList = new ArrayList[Mutation]
          iterator.foreach(T => {
            mutationList.add(f(T))
            if (mutationList.size >= batchSize) {
              htable.batch(mutationList)
              mutationList.clear()
            }
          })
          if (mutationList.size() > 0) {
            htable.batch(mutationList)
            mutationList.clear()
          }
          htable.close()
        }))
  }

  def bulkGet[K, U: ClassTag](tableName: String,
    batchSize: Integer,
    rdd: RDD[K],
    makeGet: (K) => Get,
    convertResult: (Result) => U): RDD[U] = {

    def mp(iterator: Iterator[K], hConnection: HConnection): Iterator[U] = {
      val htable = hConnection.getTable(tableName)

      val gets = new ArrayList[Get]()
      var res = List[U]()

      while (iterator.hasNext) {
        gets.add(makeGet(iterator.next))

        if (gets.size() == batchSize) {
          var results = htable.get(gets)
          res = res ++ results.map(convertResult)
          gets.clear()
        }
      }
      if (gets.size() > 0) {
        val results = htable.get(gets)
        res = res ++ results.map(convertResult)
        gets.clear()
      }
      htable.close()
      res.iterator
      
    }

    rdd.mapPartitions[U](it => hbaseMapPartition[K, U](broadcastedConf,
      it,
      mp), true)
  }

  def distributedScan(tableName: String, scans: Scan): RDD[(Array[Byte], java.util.List[(Array[Byte], Array[Byte], Array[Byte])])] = {
    distributedScan[(Array[Byte], java.util.List[(Array[Byte], Array[Byte], Array[Byte])])](
      tableName,
      scans,
      (r: (ImmutableBytesWritable, Result)) => {
        val it = r._2.list().iterator()
        val list = new ArrayList[(Array[Byte], Array[Byte], Array[Byte])]()

        while (it.hasNext()) {
          val kv = it.next()
          list.add((kv.getFamily(), kv.getQualifier(), kv.getValue()))
        }

        (r._1.copyBytes(), list)
      })
  }

  def distributedScan[U: ClassTag]( tableName: String, scan: Scan, f: ((ImmutableBytesWritable, Result)) => U): RDD[U] = {

    var job: Job = new Job(broadcastedConf.value.value)

    TableMapReduceUtil.initTableMapperJob(tableName, scan, classOf[IdentityTableMapper], null, null, job)
    
    sc.newAPIHadoopRDD(job.getConfiguration(),
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]).map(f)
  }

  private def hbaseForeachPartition[T](configBroadcast: Broadcast[SerializableWritable[Configuration]],
    it: Iterator[T],
    f: (Iterator[T], HConnection) => Unit) = {

    val config = configBroadcast.value.value
    
    val hConnection = HConnectionStaticCache.getHConnection(config)
    try {
      f(it, hConnection)
    } finally {
      HConnectionStaticCache.finishWithHConnection(config, hConnection)
    }
  }

  private def hbaseMapPartition[K, U](configBroadcast: Broadcast[SerializableWritable[Configuration]],
    it: Iterator[K],
    mp: (Iterator[K], HConnection) => Iterator[U]): Iterator[U] = {

    val config = configBroadcast.value.value
    
    val hConnection = HConnectionStaticCache.getHConnection(config)

    try {
      val res = mp(it, hConnection)
      res
    } finally {
      HConnectionStaticCache.finishWithHConnection(config, hConnection)
    }
  }
  
  
  }