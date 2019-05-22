package com.usst.sparksql.dataframe;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class CreateDFFromMysql {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("mysql");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		/**
		 * 第一种方式读取MySql数据库表，加载为DataFrame
		 */
		Map<String, String> options = new HashMap<String, String>();
		options.put("url", "jdbc:mysql://192.168.230.121:3306/spark");
		options.put("driver", "com.mysql.jdbc.Driver");
		options.put("user", "root");
		options.put("password", "123456");
		options.put("dbtable", "person");
		DataFrame person = sqlContext.read().format("jdbc").options(options).load();
		person.show();
		person.registerTempTable("person1");

		/**
		 * 第二种方式读取MySql数据表加载为DataFrame
		 */
		DataFrameReader reader = sqlContext.read().format("jdbc");
		reader.option("url", "jdbc:mysql://192.168.230.121:3306/spark");
		reader.option("driver", "com.mysql.jdbc.Driver");
		reader.option("user", "root");
		reader.option("password", "123456");
		reader.option("dbtable", "score");
		DataFrame score = reader.load();
		//score.show();
		score.registerTempTable("score1");

		//DataFrame results = sqlContext.sql("select * from person1 p join score1 s p.id = s.id");
		DataFrame results = sqlContext.sql("SELECT * FROM person1 p JOIN score1 s ON  p.id = s.id and p.id = 1");
		results.show();

		/*
		 * JavaRDD<Person> personList = results.javaRDD().map(new Function<Row,
		 * Person>() {
		 * 
		 *//**
			* 
			*/
		/*
		 * private static final long serialVersionUID = 1L;
		 * 
		 * @Override public Person call(Row record) throws Exception { Person person =
		 * new Person(); person.setId(record.getInt(0));
		 * person.setName(record.getString(1)); person.setAge(record.getInt(2));
		 * 
		 * return person; } });
		 * 
		 * personList.foreach(new VoidFunction<Person>() {
		 * 
		 *//**
			* 
			*//*
				 * private static final long serialVersionUID = 1L;
				 * 
				 * @Override public void call(Person p) throws Exception {
				 * System.out.println(p);
				 * 
				 * } });
				 */

		sc.stop();

	}

}
