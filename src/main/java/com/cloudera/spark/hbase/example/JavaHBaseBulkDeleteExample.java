package com.cloudera.spark.hbase.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import com.cloudera.spark.hbase.JavaHBaseContext;

public class JavaHBaseBulkDeleteExample {
  public static void main(String args[]) {
    if (args.length == 0) {
      System.out.println("JavaHBaseBulkDeleteExample  {master} {tableName} ");
    }

    String master = args[0];
    String tableName = args[1];

    JavaSparkContext jsc = new JavaSparkContext(master,
        "JavaHBaseBulkDeleteExample");
    jsc.addJar("spark.jar");

    List<byte[]> list = new ArrayList<byte[]>();
    list.add(Bytes.toBytes("1"));
    list.add(Bytes.toBytes("2"));
    list.add(Bytes.toBytes("3"));
    list.add(Bytes.toBytes("4"));
    list.add(Bytes.toBytes("5"));

    JavaRDD<byte[]> rdd = jsc.parallelize(list);

    Configuration conf = HBaseConfiguration.create();
    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));

    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

    hbaseContext.bulkDelete(rdd, tableName, new DeleteFunction(), 4);

  }

  public static class DeleteFunction implements Function<byte[], Delete> {

    private static final long serialVersionUID = 1L;

    public Delete call(byte[] v) throws Exception {

      return new Delete(v);
    }

  }
}
