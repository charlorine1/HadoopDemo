package com.usst.spark.transformations;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;





/** 
 *  方法描述： 不管是怎么样的RDD调用该算子，其实生成的都是一个tuple二元组
 *            
 *      JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD 来调用也是生成一个tuple<String,String> 的二元组，并不是想有些算子只能有一部分的RDD来调用
 * 
 * **/
public class Operator_mapToPair {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("mapToPair");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> lines = jsc.textFile("./data/words.txt");
		JavaRDD<String> flatMap = lines.flatMap(new FlatMapFunction<String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String s) throws Exception {
				return Arrays.asList(s.split(" "));
			}
		});
		JavaPairRDD<String, Integer> result = flatMap.mapToPair(new PairFunction<String, String, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String,Integer>(t,1);
			}
		});
		
		result.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t._1+"...."+t._2);
			}
		});
		
		jsc.stop();
	}

}

/**
=================================================================================
			hello....1
			wordl....1
			hello....1
			usst....1
			hello....1
			haha....1
			hello....1
			usst....1
=================================================================================
**/

