package com.cloudera.spark.hbase.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import com.cloudera.spark.hbase.JavaHBaseContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class JavaHBaseStreamingBulkPutExample {
  public static void main(String args[]) {
    if (args.length == 0) {
      System.out
          .println("JavaHBaseBulkPutExample  {master} {host} {post} {tableName} {columnFamily}");
    }

    String master = args[0];
    String host = args[1];
    String port = args[2];
    String tableName = args[3];
    String columnFamily = args[4];

    System.out.println("master:" + master);
    System.out.println("host:" + host);
    System.out.println("port:" + Integer.parseInt(port));
    System.out.println("tableName:" + tableName);
    System.out.println("columnFamily:" + columnFamily);
    
    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.cleaner.ttl", "120000");
    
    JavaSparkContext jsc = new JavaSparkContext(master,
        "JavaHBaseBulkPutExample");
    jsc.addJar("spark.jar");
    
    JavaStreamingContext jssc = new JavaStreamingContext(jsc, new Duration(1000));

    JavaReceiverInputDStream<String> javaDstream = jssc.socketTextStream(host, Integer.parseInt(port));
    
    Configuration conf = HBaseConfiguration.create();
    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));

    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

    hbaseContext.streamBulkPut(javaDstream, tableName, new PutFunction(), true);
  }

  public static class PutFunction implements Function<String, Put> {

    private static final long serialVersionUID = 1L;

    public Put call(String v) throws Exception {
      String[] cells = v.split(",");
      Put put = new Put(Bytes.toBytes(cells[0]));

      put.add(Bytes.toBytes(cells[1]), Bytes.toBytes(cells[2]),
          Bytes.toBytes(cells[3]));
      return put;
    }

  }
}
