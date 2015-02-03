package com.cloudera.spark.hbase.example;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import com.cloudera.spark.hbase.JavaHBaseContext;
import org.junit.After;

import scala.Tuple2;

import com.google.common.io.Files;

public class TestJavaLocalMainExample {
  
  private static transient JavaSparkContext jsc;
  private static transient File tempDir;
  static HBaseTestingUtility htu;

  static String tableName = "t1";
  static String columnFamily = "c";
  
  public static void main(String[] agrs) {
    setUp();
    
    Configuration conf = htu.getConfiguration();

    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
    
    List<byte[]> list = new ArrayList<byte[]>();
    list.add(Bytes.toBytes("1"));
    list.add(Bytes.toBytes("2"));
    list.add(Bytes.toBytes("3"));
    list.add(Bytes.toBytes("4"));
    list.add(Bytes.toBytes("5"));

    JavaRDD<byte[]> rdd = jsc.parallelize(list);
 

    hbaseContext.foreachPartition(rdd,  new VoidFunction<Tuple2<Iterator<byte[]>, HConnection>>() {


      public void call(Tuple2<Iterator<byte[]>, HConnection> t)
              throws Exception {
        HTableInterface table1 = t._2().getTable(Bytes.toBytes("Foo"));

        Iterator<byte[]> it = t._1();

        while (it.hasNext()) {
          byte[] b = it.next();
          Result r = table1.get(new Get(b));
          if (r.getExists()) {
            table1.put(new Put(b));
          }
        }
      }
    });

    //This is me
    hbaseContext.foreach(rdd, new VoidFunction<Tuple2<byte[], HConnection>>() {

      public void call(Tuple2<byte[], HConnection> t)
          throws Exception {
        HTableInterface table1 = t._2().getTable(Bytes.toBytes("Foo"));
        
        byte[] b = t._1();
        Result r = table1.get(new Get(b));
        if (r.getExists()) {
          table1.put(new Put(b));
        }
        
      }
    });
    
    tearDown();
  }
  
  public static void setUp() {
    jsc = new JavaSparkContext("local", "JavaHBaseContextSuite");
    jsc.addJar("spark.jar");
    
    tempDir = Files.createTempDir();
    tempDir.deleteOnExit();

    htu = HBaseTestingUtility.createLocalHTU();
    try {
      System.out.println("cleaning up test dir");

      htu.cleanupTestDir();

      System.out.println("starting minicluster");

      htu.startMiniZKCluster();
      htu.startMiniHBaseCluster(1, 1);

      System.out.println(" - minicluster started");

      try {
        htu.deleteTable(Bytes.toBytes(tableName));
      } catch (Exception e) {
        System.out.println(" - no table " + tableName + " found");
      }

      System.out.println(" - creating table " + tableName);
      htu.createTable(Bytes.toBytes(tableName), Bytes.toBytes(columnFamily));
      System.out.println(" - created table");
    } catch (Exception e1) {
      throw new RuntimeException(e1);
    }
  }

  @After
  public static void tearDown() {
    try {
      htu.deleteTable(Bytes.toBytes(tableName));
      System.out.println("shuting down minicluster");
      htu.shutdownMiniHBaseCluster();
      htu.shutdownMiniZKCluster();
      System.out.println(" - minicluster shut down");
      htu.cleanupTestDir();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    jsc.stop();
    jsc = null;
  }
}
