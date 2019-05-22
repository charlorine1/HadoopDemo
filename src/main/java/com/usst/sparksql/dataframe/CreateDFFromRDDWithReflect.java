package com.usst.sparksql.dataframe;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class CreateDFFromRDDWithReflect {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf();

		conf.setMaster("local").setAppName("createDFFormatRDDwithReflet");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		SQLContext sqlContext = new SQLContext(jsc);

		// Load a text file and convert each line to a JavaBean.
		JavaRDD<Person> people = jsc.textFile("./sparksql/person.txt").map(new Function<String, Person>() {
			/**
			* 
			*/
			private static final long serialVersionUID = 1L;

			public Person call(String line) throws Exception {
				String[] parts = line.split(",");

				Person person = new Person();
				person.setId(Integer.parseInt(parts[0]));
				person.setName(parts[1]);
				person.setAge(Integer.parseInt(parts[2].trim()));

				return person;
			}
		});

		// Apply a schema to an RDD of JavaBeans and register it as a table.
		DataFrame schemaPeople = sqlContext.createDataFrame(people, Person.class);

		schemaPeople.show();

		schemaPeople.registerTempTable("person");

		DataFrame results = sqlContext.sql("select id, name from person");

		List<String> person = results.javaRDD().map(new Function<Row, String>() {
			/**
			* 
			*/
			private static final long serialVersionUID = 1L;

			public String call(Row row) {
				return "id: " + row.getInt(0)+"------------------Name: " + row.getString(1);
			}
		}).collect();

		System.out.println("person list = " + person);

		jsc.stop();

	}

}
