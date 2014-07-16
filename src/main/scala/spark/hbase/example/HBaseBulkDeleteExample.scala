package spark.hbase.example

import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.util.Bytes
import spark.hbase.HBaseContext
import org.apache.hadoop.hbase.client.Delete

object HBaseBulkDeleteExample {
  def main(args: Array[String]) {
	  if (args.length == 0) {
    		System.out.println("HBaseBulkDeletesExample {master} {tableName} {columnFamily}");
    		return;
      }
    	
      val master = args(0);
      val tableName = args(1);
    	
      val sc = new SparkContext(master, "HBaseBulkDeletesExample");
      sc.addJar("SparkHBase.jar")
      
      //[Array[Byte]]
      val rdd = sc.parallelize(Array(
            (Bytes.toBytes("1")),
            (Bytes.toBytes("2")),
            (Bytes.toBytes("3")),
            (Bytes.toBytes("4")),
            (Bytes.toBytes("5"))
           )
          );
    	
      val conf = HBaseConfiguration.create();
	    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
	    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
    	
      val hbaseContext = new HBaseContext(conf);
      hbaseContext.bulkDeletes[Array[Byte]](rdd, 
          tableName,
          putRecord => new Delete(putRecord),
          true);
	}
}