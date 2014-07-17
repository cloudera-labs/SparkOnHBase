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

@serializable class HBaseContext(@transient var conf: Configuration) {

  def this() = this(null)

  var configBytes: Array[Byte] = Option(conf).map { c =>
    val baos = new ByteArrayOutputStream()
    c.write(new DataOutputStream(baos))
    val array = baos.toByteArray()
    baos.close()
    array
    
  }.orNull
  
  

  def foreachPartition[T](rdd: RDD[T], f: (Iterator[T], HConnection) => Unit) = {
    rdd.foreachPartition(it => hbaseForeachPartitionFunction(configBytes, it, f))
  }

  def mapPartition[T, U: ClassTag] (rdd: RDD[T], 
      mp: (Iterator[T], HConnection) => Iterator[U]): RDD[U] = {
    
    rdd.mapPartitions[U]( it => hbaseBatchGetMapIterator[T, U](configBytes,
        it,
        mp), true)
  }
  
  def bulkPuts[T](rdd: RDD[T], tableName: String, f: (T) => Put, autoFlush: Boolean) {
    
    rdd.foreachPartition(
      it => hbaseForeachPartitionFunction[T](
        configBytes,
        it,
        (iterator, hConnection) => {
          var htable = hConnection.getTable(tableName)
          htable.setAutoFlush(autoFlush, true)
          iterator.foreach(T => htable.put(f(T)))
          htable.close()
        }))
  }

  def bulkIncrements[T](rdd: RDD[T], tableName: String, f: (T) => Increment, autoFlush: Boolean) {
    rdd.foreachPartition(
      it => hbaseForeachPartitionFunction[T](
        configBytes,
        it,
        (iterator, hConnection) => {
          var htable = hConnection.getTable(tableName)
          htable.setAutoFlush(autoFlush, true)
          iterator.foreach(T => htable.increment(f(T)))
          htable.close()
        }))
  }

  def bulkDeletes[T](rdd: RDD[T], tableName: String, f: (T) => Delete, autoFlush: Boolean) {
    rdd.foreachPartition(
      it => hbaseForeachPartitionFunction[T](
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
    batchSize: Integer,
    rdd: RDD[K],
    makeGet: (K) => Get,
    convertResult: (Result) => U): RDD[U] = {
    
    def mp(iterator: Iterator[K], hConnection: HConnection):Iterator[U] = {
          var htable = hConnection.getTable(tableName)

          var gets = new ArrayList[Get]()
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
            var results = htable.get(gets)
            res = res ++ results.map(convertResult)
            gets.clear()
          }
          res.iterator
        }  
    
    rdd.mapPartitions[U]( it => hbaseBatchGetMapIterator[K, U](configBytes,
        it,
        mp), true)
  }

  def distributedScan(sc: SparkContext, tableName: String, scans: Scan): RDD[(Array[Byte], java.util.List[(Array[Byte], Array[Byte], Array[Byte])])] = {
    distributedScan[(Array[Byte], java.util.List[(Array[Byte], Array[Byte], Array[Byte])])](
      sc,
      tableName,
      scans,
      (r: (ImmutableBytesWritable, Result)) => {
        val it = r._2.list().iterator()
        var list = new ArrayList[(Array[Byte], Array[Byte], Array[Byte])]()
        
        while (it.hasNext()) {
          var kv = it.next()
          list.add((kv.getFamily(), kv.getQualifier(), kv.getValue()))
        }
        
        (r._1.copyBytes(), list)
      })
  }

  def distributedScan[U: ClassTag](sc: SparkContext, tableName: String, scan: Scan , f: ((ImmutableBytesWritable, Result)) => U): RDD[U] = {

    var job: Job = new Job(deserializeConfig(configBytes))

    TableMapReduceUtil.initTableMapperJob(tableName, scan, classOf[IdentityTableMapper], null, null, job)

    System.out.println("hbase.mapreduce.inputtable.1.2: " + job.getConfiguration().get("hbase.mapreduce.inputtable"))
    
    sc.newAPIHadoopRDD(job.getConfiguration(),
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]).map(f)
  }

  private def hbaseForeachPartitionFunction[T](hbaseConf: Array[Byte],
    it: Iterator[T],
    f: (Iterator[T], HConnection) => Unit) = {

    val config = deserializeConfig(hbaseConf)

    var hConnection = HConnectionManager.createConnection(config)

    f(it, hConnection)

    hConnection.close()
  }

  private def hbaseBatchGetMapIterator[K, U](hbaseConf: Array[Byte],
    it: Iterator[K],
    mp: (Iterator[K], HConnection) => Iterator[U]): Iterator[U] = {

    val config = deserializeConfig(hbaseConf)

    var hConnection = HConnectionManager.createConnection(config)

    var res = mp(it, hConnection)

    hConnection.close()
    res
  }

  private def deserializeConfig(hbaseConf: Array[Byte]): Configuration = {
    val bais = new ByteArrayInputStream(hbaseConf)
    val in = new DataInputStream(bais)
    val config = new Configuration()
    config.readFields(in)
    bais.close()
    config
  }
}