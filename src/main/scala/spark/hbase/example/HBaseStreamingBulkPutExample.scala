package spark.hbase.example

import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import spark.hbase.HBaseContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object HBaseStreamingBulkPutExample {
  def main(args: Array[String]) {
    if (args.length == 0) {
        System.out.println("HBaseStreamingBulkPutExample {master} {host} {port} {tableName} {columnFamily}");
        return;
      }
      
      val master = args(0);
      val host = args(1);
      val port = args(2);
      val tableName = args(3);
      val columnFamily = args(4);
      val driverPort = args(5);
      
      System.out.println("master:" + master)
      System.out.println("host:" + host)
      System.out.println("port:" + Integer.parseInt(port))
      System.out.println("tableName:" + tableName)
      System.out.println("columnFamily:" + columnFamily)
      
      val sc = new SparkContext(master, "HBaseStreamingBulkPutExample")
      sc.addJar("SparkHBase.jar")
      
      sc.getConf.set("spark.master.port", driverPort);
      sc.getConf.set("spark.driver.port", driverPort);
      
      val ssc = new StreamingContext(sc.getConf, Seconds(1))
      
      val lines = ssc.socketTextStream(host, Integer.parseInt(port))
      
      val conf = HBaseConfiguration.create();
      conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
      conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
      
      val hbaseContext = new HBaseContext(sc, conf);
      
      hbaseContext.streamBulkPut[String](lines, 
          tableName,
          (putRecord) => {
            val put = new Put(Bytes.toBytes(putRecord))
            put.add(Bytes.toBytes("c"), Bytes.toBytes("foo"), Bytes.toBytes("bar"))
            put
          },
          true);
      
      ssc.start();
      
      
  }
}