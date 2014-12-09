package com.cloudera.spark.hbase.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import com.cloudera.spark.hbase.JavaHBaseContext;

public class JavaHBaseBulkIncrementExample {
  public static void main(String args[]) {
    if (args.length == 0) {
      System.out
          .println("JavaHBaseBulkIncrementExample  {master} {tableName} {columnFamily}");
    }

    String master = args[0];
    String tableName = args[1];
    String columnFamily = args[2];

    JavaSparkContext jsc = new JavaSparkContext(master,
        "JavaHBaseBulkIncrementExample");
    jsc.addJar("spark.jar");

    List<String> list = new ArrayList<String>();
    list.add("1," + columnFamily + ",counter,1");
    list.add("2," + columnFamily + ",counter,2");
    list.add("3," + columnFamily + ",counter,3");
    list.add("4," + columnFamily + ",counter,4");
    list.add("5," + columnFamily + ",counter,5");

    JavaRDD<String> rdd = jsc.parallelize(list);

    Configuration conf = HBaseConfiguration.create();
    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));

    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

    hbaseContext.bulkIncrement(rdd, tableName, new IncrementFunction(), 4);

  }

  public static class IncrementFunction implements Function<String, Increment> {

    private static final long serialVersionUID = 1L;

    public Increment call(String v) throws Exception {
      String[] cells = v.split(",");
      Increment increment = new Increment(Bytes.toBytes(cells[0]));

      increment.addColumn(Bytes.toBytes(cells[1]), Bytes.toBytes(cells[2]),
          Integer.parseInt(cells[3]));
      return increment;
    }

  }
}
