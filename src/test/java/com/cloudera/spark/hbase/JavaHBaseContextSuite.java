package com.cloudera.spark.hbase;

import java.io.File;
import java.io.Serializable;

import com.cloudera.spark.hbase.example.JavaHBaseBulkDeleteExample.DeleteFunction;
import com.cloudera.spark.hbase.example.JavaHBaseBulkIncrementExample.IncrementFunction;

import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import com.cloudera.spark.hbase.JavaHBaseContext;
import org.junit.*;

import scala.Tuple2;
import scala.Tuple3;

import com.google.common.io.Files;

public class JavaHBaseContextSuite implements Serializable {
  private transient JavaSparkContext jsc;
  private transient File tempDir;
  HBaseTestingUtility htu;

  String tableName = "t1";
  String columnFamily = "c";

  @Before
  public void setUp() {
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
  public void tearDown() {
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

  @Test
  public void testJavaBulkIncrement() {
    
    List<String> list = new ArrayList<String>();
    list.add("1," + columnFamily + ",counter,1");
    list.add("2," + columnFamily + ",counter,2");
    list.add("3," + columnFamily + ",counter,3");
    list.add("4," + columnFamily + ",counter,4");
    list.add("5," + columnFamily + ",counter,5");

    JavaRDD<String> rdd = jsc.parallelize(list);

    Configuration conf = htu.getConfiguration();

    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

    hbaseContext.bulkIncrement(rdd, tableName, new IncrementFunction(), 4);
    
    throw new RuntimeException();
  }
  
  

  @Test
  public void testBulkPut() {
    
    List<String> list = new ArrayList<String>();
    list.add("1," + columnFamily + ",a,1");
    list.add("2," + columnFamily + ",a,2");
    list.add("3," + columnFamily + ",a,3");
    list.add("4," + columnFamily + ",a,4");
    list.add("5," + columnFamily + ",a,5");

    JavaRDD<String> rdd = jsc.parallelize(list);

    Configuration conf = htu.getConfiguration();

    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

    hbaseContext.bulkPut(rdd, tableName, new PutFunction(), true);
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
  
  @Test
  public void testBulkDelete() {
    List<byte[]> list = new ArrayList<byte[]>();
    list.add(Bytes.toBytes("1"));
    list.add(Bytes.toBytes("2"));
    list.add(Bytes.toBytes("3"));
    list.add(Bytes.toBytes("4"));
    list.add(Bytes.toBytes("5"));

    JavaRDD<byte[]> rdd = jsc.parallelize(list);

    Configuration conf = htu.getConfiguration();

    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

    hbaseContext.bulkDelete(rdd, tableName, new DeleteFunction(), 4);
  }
  
  @Test
  public void testDistributedScan() {
    Configuration conf = htu.getConfiguration();
    
    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

    Scan scan = new Scan();
    scan.setCaching(100);
    
    JavaRDD<Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>>> javaRdd = hbaseContext.hbaseRDD(tableName, scan);
    
    List<Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>>> results = javaRdd.collect();
    
    results.size();
  }
  
  @Test
  public void testBulkGet() {
    List<byte[]> list = new ArrayList<byte[]>();
    list.add(Bytes.toBytes("1"));
    list.add(Bytes.toBytes("2"));
    list.add(Bytes.toBytes("3"));
    list.add(Bytes.toBytes("4"));
    list.add(Bytes.toBytes("5"));

    JavaRDD<byte[]> rdd = jsc.parallelize(list);

    Configuration conf = htu.getConfiguration();

    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

    hbaseContext.bulkGet(tableName, 2, rdd, new GetFunction(),
        new ResultFunction());
  }

  public static class GetFunction implements Function<byte[], Get> {

    private static final long serialVersionUID = 1L;

    public Get call(byte[] v) throws Exception {
      return new Get(v);
    }
  }

  public static class ResultFunction implements Function<Result, String> {

    private static final long serialVersionUID = 1L;

    public String call(Result result) throws Exception {
      Iterator<KeyValue> it = result.list().iterator();
      StringBuilder b = new StringBuilder();

      b.append(Bytes.toString(result.getRow()) + ":");

      while (it.hasNext()) {
        KeyValue kv = it.next();
        String q = Bytes.toString(kv.getQualifier());
        if (q.equals("counter")) {
          b.append("(" + Bytes.toString(kv.getQualifier()) + ","
              + Bytes.toLong(kv.getValue()) + ")");
        } else {
          b.append("(" + Bytes.toString(kv.getQualifier()) + ","
              + Bytes.toString(kv.getValue()) + ")");
        }
      }
      return b.toString();
    }
  }

}
