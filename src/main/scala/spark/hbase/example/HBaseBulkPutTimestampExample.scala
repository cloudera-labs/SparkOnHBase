package spark.hbase.example

import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import spark.hbase.HBaseContext

object HBaseBulkPutTimestampExample {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.out.println("HBaseBulkPutTimestampExample {master} {tableName} {columnFamily}");
      return ;
    }

    val master = args(0);
    val tableName = args(1);
    val columnFamily = args(2);
    

    val sc = new SparkContext(master, "HBaseBulkPutTimestampExample");
    sc.addJar("SparkHBase.jar")

    val rdd = sc.parallelize(Array(
      (Bytes.toBytes("6"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("1")))),
      (Bytes.toBytes("7"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("2")))),
      (Bytes.toBytes("8"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("3")))),
      (Bytes.toBytes("9"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("4")))),
      (Bytes.toBytes("10"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("5"))))));

    val conf = HBaseConfiguration.create();
    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));

    var timeStamp = System.currentTimeMillis()
    
    val hbaseContext = new HBaseContext(conf);
    hbaseContext.bulkPuts[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](rdd,
      tableName,
      (putRecord) => {
        var put = new Put(putRecord._1)
        putRecord._2.foreach((putValue) => put.add(putValue._1, putValue._2, timeStamp, putValue._3))
        put
      },
      true);
  }
}