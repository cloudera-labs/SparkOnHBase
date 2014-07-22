package spark.hbase.example

import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import spark.hbase.HBaseContext
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text

object HBaseBulkPutExampleFromFile {
  def main(args: Array[String]) {
	  if (args.length == 0) {
    		System.out.println("HBaseBulkPutExample {master} {tableName} {columnFamily}");
    		return;
      }
    	
      val master = args(0)
      val tableName = args(1)
      val columnFamily = args(2)
      val inputFile = args(3)
    	
      val sc = new SparkContext(master, "HBaseBulkPutExample");
      sc.addJar("SparkHBase.jar")
      
      var rdd = sc.hadoopFile(
          inputFile, 
          classOf[TextInputFormat], 
          classOf[LongWritable], 
          classOf[Text]).map(v => {
            System.out.println("reading-" + v._2.toString())
            v._2.toString()
          })

      val conf = HBaseConfiguration.create();
	    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
	    conf.addResource(new Path("/etc/hbase/conf/hdfs-site.xml"));
	    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
    	
      val hbaseContext = new HBaseContext(sc, conf);
      hbaseContext.bulkPut[String](rdd, 
          tableName,
          (putRecord) => {
            System.out.println("hbase-" + putRecord)
            val put = new Put(Bytes.toBytes("Value- " + putRecord))
            put.add(Bytes.toBytes("c"), Bytes.toBytes("1"), Bytes.toBytes(putRecord.length()))
            put
          },
          true);
	}
}