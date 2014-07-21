package spark.hbase.debugging

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf

object SimpleStreamingExample {
  def main(args: Array[String]) {
    if (args.length == 0) {
        System.out.println("SimpleStreamingExample {master} {host} {port} ");
        return;
      } 
      
      val master = args(0);
      val host = args(1);
      val port = args(2);
      val akkaPort = args(3);
      
      System.out.println("master:" + master)
      System.out.println("host:" + host)
      System.out.println("port:" + Integer.parseInt(port))
      
      val sparkConf = new SparkConf();
      
      sparkConf.set("spark.driver.port", akkaPort)
      
      val sc = new SparkContext(master, "SimpleStreamingExample", sparkConf)
      sc.addJar("SparkHBase.jar")
      
      val ssc = new StreamingContext(sc.getConf, Seconds(1))
      
      val lines = ssc.socketTextStream(host, Integer.parseInt(port))
      
      lines.foreach(rdd => { rdd.foreach(s => println(s))})
      
      ssc.start();
      
      
  }
}