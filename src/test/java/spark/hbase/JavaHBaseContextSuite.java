package spark.hbase;

import java.io.File;
import java.io.Serializable;

import com.google.common.collect.Lists;
import java.util.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.*;

import com.google.common.io.Files;

public class JavaHBaseContextSuite implements Serializable  {
	private transient JavaSparkContext sc;
	private transient File tempDir;

	
	@Before
	public void setUp() {
		sc = new JavaSparkContext("local", "JavaHBaseContextSuite");
		tempDir = Files.createTempDir();
		tempDir.deleteOnExit();
		
	}

	@After
	public void tearDown() {
		sc.stop();
		sc = null;
	}

	@Test
	public void testBulkIncrement() {
		System.out.println("Java Test bulkIncrement");
		List<String> strings = Arrays.asList("1|c|counter|1", 
				"2|c|counter|2",
				"3|c|counter|3",
				"4|c|counter|4",
				"5|c|counter|5");
		
		JavaRDD<String> s1 = sc.parallelize(strings);
		
		System.out.println("Java Test bulkIncrement: stop");
		int i = 0;
		i = i/i;
	}
	
	@Test
	public void testBulkIncrement2() {
		System.out.println("Java Test bulkIncrement2");
		List<String> strings = Arrays.asList("1|c|counter|1", 
				"2|c|counter|2",
				"3|c|counter|3",
				"4|c|counter|4",
				"5|c|counter|5");
		
		JavaRDD<String> s1 = sc.parallelize(strings);
		Assert.assertEquals(4, 2);
		
	}
}
