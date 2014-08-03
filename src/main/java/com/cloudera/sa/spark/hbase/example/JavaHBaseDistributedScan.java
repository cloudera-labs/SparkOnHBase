package com.cloudera.sa.spark.hbase.example;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.hbase.HBaseContext;

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
    jsc.addJar("SparkHBase.jar");


    Configuration conf = HBaseConfiguration.create();
    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));

    HBaseContext hbaseContext = new HBaseContext(jsc.sc(), conf);

    Scan scan = new Scan();
    scan.setCaching(100);
    
    JavaRDD javaRdd = hbaseContext.javaHBaseRDD(tableName, scan);
    
    List results = javaRdd.collect();
  }
}
