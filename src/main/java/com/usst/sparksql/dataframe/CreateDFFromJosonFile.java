package com.usst.sparksql.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;


/**
 * 读取json格式的文件创建DataFrame
 * 
 * 注意 ：json文件中不能嵌套json格式的内容
 * @author root
 *
 */
public class CreateDFFromJosonFile {

	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("jsonfile");
		SparkContext sc = new SparkContext(conf);
		
		//创建sqlContext
		SQLContext sqlContext = new SQLContext(sc);
		
		/**
		 * DataFrame的底层是一个一个的RDD  RDD的泛型是Row类型。
		 * 以下两种方式都可以读取json格式的文件
		 */
//		DataFrame df = sqlContext.read().format("json").load("sparksql/json");
		DataFrame df2 = sqlContext.read().json("./sparksql/json");
		df2.show();
		/**
		 * 显示 DataFrame中的内容，默认显示前20行。如果现实多行要指定多少行show(行数)
		 * 注意：当有多个列时，显示的列先后顺序是按列的ascii码先后显示。
		 */
//		df.show();
		/**
		 * DataFrame转换成RDD
		 */
//		RDD<Row> rdd = df.rdd();
		/**
		 * 树形的形式显示schema信息
		 */
      	df2.printSchema();
		
		/**
		 * dataFram自带的API 操作DataFrame
		 */
		//select name from table
//		df.select("name").show();
		//select name ,age+10 as addage from table
//		df.select(df.col("name"),df.col("age").plus(10).alias("addage")).show();
		//select name ,age from table where age>19
//		df.select(df.col("name"),df.col("age")).where(df.col("age").gt(19)).show();
		//select age,count(*) from table group by age
//		df.groupBy(df.col("age")).count().show();
		
		/**
		 * 将DataFrame注册成临时的一张表，这张表临时注册到内存中，是逻辑上的表，不会雾化到磁盘
		 */
		df2.registerTempTable("jtable");
		
		DataFrame sql = sqlContext.sql("select age,count(*) as gg from jtable group by age");
		sql.show();
//		DataFrame sql2 = sqlContext.sql("select * from jtable");
//		sql2.show();
		sc.stop();
	}

}
