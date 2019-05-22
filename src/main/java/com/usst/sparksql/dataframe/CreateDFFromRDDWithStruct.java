package com.usst.sparksql.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * 动态创建Schema将非json格式RDD转换成DataFrame
 * 
 * 作用：将javaRdd
 * 
 * @author root
 *
 */
public class CreateDFFromRDDWithStruct {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("rddStruct");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		JavaRDD<String> lineRDD = sc.textFile("./sparksql/person.txt");

		/**
		 * 将 RDD 转换程ROW （JavaRDD<String> 变成JavaRDD<Row>）
		 * 一个row  是 table的一行数据，一个map执行完成就把所有行都变为数据库的一行，放到JavaRDD<Row> rowRDD里面
		 */
		// Convert records of the RDD (people) to Rows.
		JavaRDD<Row> rowRDD = lineRDD.map(new Function<String, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Row call(String record) throws Exception {
				String[] fields = record.split(",");
				return RowFactory.create(fields[0], fields[1].trim(), Integer.valueOf(fields[2]));
			}
		});

		// 生成一个schema ，Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("id", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
		StructType schema = DataTypes.createStructType(fields);

		// Apply the schema to the RDD.
		DataFrame peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema);

		peopleDataFrame.registerTempTable("people");

		peopleDataFrame.show();

		DataFrame results = sqlContext.sql("select * from people");
		
		results.show();

		// The results of SQL queries are DataFrames and support all the normal RDD
		// operations.
		// The columns of a row in the result can be accessed by ordinal.
		List<String> names = results.javaRDD().map(new Function<Row, String>() {

			private static final long serialVersionUID = 1L;

			public String call(Row row) {
				return "Name: " + row.getString(0);
			}
		}).collect();
		
		System.out.println("List<String> name="+names);
		
		
		
		sc.stop();

	}

}
