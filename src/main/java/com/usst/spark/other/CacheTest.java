package com.usst.spark.other;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;


/**
 * 1、cache() persist() 是懒执行，要用action类算子触发，持久化的单位是partion
 *                     有3个patition ，内存个200M ，  如果内存是500M，则分布是：内存400M， 磁盘200M
 * 2、cache() persist() 对RDD持久化之后赋值给一个变量，那么下次使用到持久化的数据时，直接使用这个变量就是使用的持久化的数据据
 * 3、cache() persist() 后面不能紧跟action算子，因为action算子执行后的结果就不是rdd了，不是rdd就不能调用算子了
 * 4、    persist的 _replication分区数？
 * 
 * */
public class CacheTest {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("cachetest");
		
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> lines = jsc.textFile("./data/frontgateway.log");
		
		//lines.cache();
		lines.persist(StorageLevel.MEMORY_AND_DISK());
	    long starttime = System.currentTimeMillis();
	    System.out.println("count1="+lines.count());
	    long endtime = System.currentTimeMillis();
		
	    System.out.println("第一次耗费时间="+(endtime - starttime));
	    
	    long startTime1 = System.currentTimeMillis();
		System.out.println("count2="+lines.count());
		long endTime1 = System.currentTimeMillis();
		
		System.out.println("第二次耗费时间="+(endTime1 -startTime1));
	    		
		jsc.close();
		
	}

}
