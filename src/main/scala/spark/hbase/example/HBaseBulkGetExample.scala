package spark.hbase.example

import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Result
import spark.hbase.HBaseContext

object HBaseBulkGetExample {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.out.println("GenerateGraphs {master} {tableName}");
      return ;
    }

    val master = args(0);
    val tableName = args(1);

    val sc = new SparkContext(master, "HBaseBulkGetExample");
    sc.addJar("SparkHBase.jar")

    //[(Array[Byte])]
    val rdd = sc.parallelize(Array(
      (Bytes.toBytes("1")),
      (Bytes.toBytes("2")),
      (Bytes.toBytes("3")),
      (Bytes.toBytes("4")),
      (Bytes.toBytes("5")),
      (Bytes.toBytes("6")),
      (Bytes.toBytes("7"))))

    val conf = HBaseConfiguration.create()
    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"))
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))

    val hbaseContext = new HBaseContext(sc, conf);
    
    val getRdd = hbaseContext.bulkGet[Array[Byte], String](
      tableName,
      2,
      rdd,
      record => { 
        System.out.println("making Get" )
        new Get(record)
      },
      (result: Result) => {

        val it = result.list().iterator()
        val B = new StringBuilder

        B.append(Bytes.toString(result.getRow()) + ":")

        while (it.hasNext()) {
          val kv = it.next()
          val q = Bytes.toString(kv.getQualifier())
          if (q.equals("counter")) {
            B.append("(" + Bytes.toString(kv.getQualifier()) + "," + Bytes.toLong(kv.getValue()) + ")")
          } else {
            B.append("(" + Bytes.toString(kv.getQualifier()) + "," + Bytes.toString(kv.getValue()) + ")")  
          }
        }
        B.toString
      })
      
    
    getRdd.collect.foreach(v => System.out.println(v))
    
  }
}