package com.usst.spark.other.accumulator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;


/**
 * 
 * 参考 ：https://www.bbsmax.com/A/kPzO0EZJxn/
 * 
 * */
public class LongAccumulatorDemo {

	public static void main(String[] args) {
		 SparkConf conf=new SparkConf().setAppName("AccumulatorDemo").setMaster("local");
	        JavaSparkContext sc=new JavaSparkContext(conf);
	 
	        Accumulator<Long> acc=sc.accumulator(20L,new LongAccumulator());
	 
	        List<Long> seq=Arrays.asList(5L,6L,7L,8L);
	        JavaRDD<Long> rdd=sc.parallelize(seq);
	 
	        rdd.foreach(new VoidFunction<Long>(){
	 
				private static final long serialVersionUID = 1L;

				@Override
	            public void call(Long arg0) throws Exception {
	                acc.add(arg0);
	            }
	 
	        });
	 
	        System.out.println("acc.value() = " + acc.value());;

	}

}
