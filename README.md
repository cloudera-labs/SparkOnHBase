# SparkOnHBase
## Overview
This is a simple reusable lib for working with HBase with Spark


##Functionality
Current functionality supports the following functions

* bulkPut
* bulkDelete
* bulkIncrement
* bulkGet
* bulkCheckAndPut
* bulkCheckAndDelete
* foreachPartition (with HConnection)
* mapPartition (with HConnection)
* hbaseRDD (HBaseInputFormat)
* Java APIs
* Java Examples
* Java Unit Tests

##Near Future
* More unit test
* More Examples
* Python APIs
* Python Examples
* Python Unit Test

##Far Future

* Partitioned Bulk Put/Mutation
* Sorted Partitioned Bulk Get
* Spark implementation of HBase bulkLoad
* Spark implementation of HBase copyTable


##Build
just mvn clean package

##CDH setup
Testing was done on CDH 5.0.2

Just put hbase-protocol-0.96.1.1-cdh5.0.2.jar in /opt/cloudera/parcels/CDH-5.0.2-1.cdh5.0.2.p0.13/lib/spark/assembly/lib/ and bunced

/opt/cloudera/parcels/CDH-5.1.0-1.cdh5.1.0.p0.53/lib/spark/assembly/lib

##Examples
SparkOnHBase comes with a number of examples.  Here is the Cli commands to try them out.  These work with a table named 't1' and columnfmaily 'c'

* java -cp SparkHBase.jar spark.hbase.example.HBaseBulkGetExample spark://{spark.master.host}:7077  t1 c
* java -cp SparkHBase.jar spark.hbase.example.HBaseBulkPutTimestampExample spark://{spark.master.host}:7077  t1 c
* java -cp SparkHBase.jar spark.hbase.example.HBaseBulkIncrementsExample spark://{spark.master.host}:7077  t1 c
* java -cp SparkHBase.jar spark.hbase.example.HBaseBulkPutExample spark://{spark.master.host}:7077  t1 c
* java -cp SparkHBase.jar spark.hbase.example.HBaseBulkDeletesExample spark://{spark.master.host}:7077 t1 c
* java -cp SparkHBase.jar spark.hbase.example.HBaseDistributedScanExample spark://tedmalaska-exp-b-1.ent.cloudera.com:7077 t1 c

sbt/sbt assembly 'test-only org.apache.spark.hbase*'


