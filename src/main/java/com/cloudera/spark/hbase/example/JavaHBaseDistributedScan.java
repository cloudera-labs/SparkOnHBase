package com.cloudera.spark.hbase.example;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import com.cloudera.spark.hbase.JavaHBaseContext;

import scala.Tuple2;
import scala.Tuple3;

public class JavaHBaseDistributedScan {
  public static void main(String args[]) {
    if (args.length == 0) {
      System.out
          .println("JavaHBaseDistributedScan  {master} {tableName}");
    }

    String master = args[0];
    String tableName = args[1];

    JavaSparkContext jsc = new JavaSparkContext(master,
        "JavaHBaseDistributedScan");
    jsc.addJar("spark.jar");


    Configuration conf = HBaseConfiguration.create();
    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));

    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

    Scan scan = new Scan();
    scan.setCaching(100);
    
    JavaRDD<Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>>> javaRdd = hbaseContext.hbaseRDD(tableName, scan);
    
    List<Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>>> results = javaRdd.collect();
    
    results.size();
  }
}
