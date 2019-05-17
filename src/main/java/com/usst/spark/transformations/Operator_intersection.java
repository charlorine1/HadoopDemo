package com.usst.spark.transformations;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
/**
 * intersection
 * 取两个RDD的交集
 * @author root
 *
 */
public class Operator_intersection {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("intersection");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a","b","c"));
		JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("a","b","f"));
		//注意使用intersection时，RDD的类型要一致
		JavaRDD<String> intersection = rdd1.intersection(rdd2);

		System.out.println(intersection.getNumPartitions());
		intersection.foreach(new VoidFunction<String>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});
		sc.stop();
	}

}

/**
=================================================================================
			a
			b
=================================================================================
**/
