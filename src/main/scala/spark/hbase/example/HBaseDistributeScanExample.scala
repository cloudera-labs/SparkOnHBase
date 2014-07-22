package spark.hbase.example

import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Result
import spark.hbase.HBaseContext
import org.apache.hadoop.hbase.client.Scan
import java.util.ArrayList


object HBaseDistributedScanExample {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.out.println("GenerateGraphs {master} {tableName}")
      return ;
    }

    val master = args(0);
    val tableName = args(1);

    val sc = new SparkContext(master, "HBaseDistributedScanExample")
    sc.addJar("SparkHBase.jar")

    val conf = HBaseConfiguration.create()
    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"))
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))

    val hbaseContext = new HBaseContext(sc, conf)
    
    var scan = new Scan()
    scan.setCaching(100)
    
    var getRdd = hbaseContext.hbaseRDD(tableName, scan)
    
    getRdd.collect.foreach(v => System.out.println(Bytes.toString(v._1)))
    
  }
}