package com.usst.spark.other.partition;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;
/**
 * 自定义分区器
 * 注意：该自定义分区器是通过对（key，value）格式的key进行分区，所以该partitionBy算子是作用在二元组上面的（tuple（key，value））
 * @author root
 *
 */
public class PartitionerTest {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("partitioner");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaPairRDD<Integer, String> nameRDD = sc.parallelizePairs(Arrays.asList(
				new Tuple2<Integer, String>(1,"zhangsan"),
				new Tuple2<Integer, String>(2,"lisi"),
				new Tuple2<Integer, String>(3,"wangwu"),
				new Tuple2<Integer, String>(4,"zhaoliu"),
				new Tuple2<Integer, String>(5,"shunqi"),
				new Tuple2<Integer, String>(6,"zhouba")
			), 2);
		
		nameRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer,String>>, Iterator<String>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Integer index, Iterator<Tuple2<Integer, String>> iter) throws Exception {
				List<String> list = new ArrayList<String>();
				while(iter.hasNext()){
					System.out.println("nameRDD partitionID = "+index+" , value = "+iter.next());
				}
				return list.iterator();
			}
		}, true).collect();
		System.out.println("******************************");
		
		nameRDD.partitionBy(new Partitioner() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public int numPartitions() {
				return 3;
			}
			
			@Override
			public int getPartition(Object key) {
				int i = (Integer) key;
				return i%3;
			}
		}).mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer,String>>, Iterator<String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Integer index, Iterator<Tuple2<Integer, String>> iter) throws Exception {
				List<String> list = new ArrayList<String>();
				while(iter.hasNext()) {
					System.out.println("partitionRDD partitionID = "+index+" , value = "+iter.next());
					//list.add("partitionRDD partitionID = "+index+" , value = "+iter.next());
				}
				return list.iterator();
			}
		}, true).collect();
		
        
		sc.stop();
		
	}

}


/**
=================================================================================
			nameRDD partitionID = 0 , value = (1,zhangsan)
			nameRDD partitionID = 0 , value = (2,lisi)
			nameRDD partitionID = 0 , value = (3,wangwu)
			
			nameRDD partitionID = 1 , value = (4,zhaoliu)
			nameRDD partitionID = 1 , value = (5,shunqi)
			nameRDD partitionID = 1 , value = (6,zhouba)
=================================================================================
**/



/**
=================================================================================
partitionRDD partitionID = 0 , value = (3,wangwu)
partitionRDD partitionID = 0 , value = (6,zhouba)
 
partitionRDD partitionID = 1 , value = (1,zhangsan)
partitionRDD partitionID = 1 , value = (4,zhaoliu)


partitionRDD partitionID = 2 , value = (2,lisi)
partitionRDD partitionID = 2 , value = (5,shunqi)
=================================================================================
**/
