package com.usst.spark.other.accumulator;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * 累加器在Driver端定义赋初始值和读取，在Executor端累加。
 * @author root
 *
 */
public class AccumulatorOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("accumulator");
		JavaSparkContext sc = new JavaSparkContext(conf);
		final Accumulator<Integer> accumulator = sc.accumulator(0);
		//accumulator.setValue(1000);
		sc.textFile("./data/words.txt").foreach(new VoidFunction<String>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String t) throws Exception {
				accumulator.add(1);
				
//				System.out.println(accumulator.value());
//              注意：accumulator.value() 在execute端不能使用，报错
			}
		});
		System.out.println(accumulator.value());
		sc.stop();

	}

}
