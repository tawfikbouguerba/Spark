package com.project.spark_T.DataSet;

import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import com.project.spark_T.model.Movie;
import static org.apache.spark.sql.functions.col;
import java.util.List;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import scala.Tuple2;

public class Datasets {

	public static void main(String[] args) {

		SparkSession session = SparkSession.builder().appName("Test")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse").master("local").getOrCreate();

		Dataset<Row> dataframe = session.read()
				.csv("..\\spark_T\\src\\main\\java\\com\\project\\spark_T\\resources\\data.csv");

		System.out.println(dataframe.schema());

		dataframe = dataframe.withColumnRenamed("_c0", " Year");

		dataframe = dataframe.withColumnRenamed("_c1", "Industry_aggregat");

		dataframe = dataframe.withColumnRenamed("_c2", "Industry_code_NZSIOC");

		dataframe.select(" Year").show();

		dataframe.createOrReplaceTempView(" Year");

		session.sql("select * from Year").show();

		dataframe.show();

		/*
		 * 
		 * 
		 * converting rdd into datasets
		 * 
		 */

		Encoder<Movie> movieEncoder = Encoders.bean(Movie.class);

		JavaRDD<Movie> rdd = session.read()
				.textFile("..\\spark_T\\src\\main\\java\\com\\project\\spark_T\\resources\\data2.txt").javaRDD()

				.map(new Function<String, Movie>() {

					@Override
					public Movie call(String arg0) throws Exception {

						String[] data = arg0.split(",");
						System.out.println("data" + data[2]);

						return new Movie(data[0], Double.parseDouble(data[1]), data[2]);
					}
				});

		Dataset<Row> data = session.createDataFrame(rdd, Movie.class);

		data.select(col("name"), col("rating"));

		data.createOrReplaceTempView("TestView");

		Dataset<Row> datafram = session.sql("select * from TestView where rating >4");

		Dataset<Movie> movieDs = datafram.map(new MapFunction<Row, Movie>() {

			@Override
			public Movie call(Row row) throws Exception {

				return new Movie(row.getString(0), row.getDouble(1), row.getAs("timestamp"));
			}
		}, movieEncoder);

		movieDs.show();

		/*
		 * 
		 * 
		 * converting loaded csv to json
		 * 
		 */

				 Dataset<Row> dataframe1= session.read().csv("..\\spark_T\\src\\main\\java\\com\\project\\spark_T\\resources\\data.csv");

				 dataframe1.write().format("json").save("..\\spark_T\\src\\main\\java\\com\\project\\spark_T\\resources\\transformation");
      

		/*
		 * 
		 * save into table
		 * 
		 * 
		 * 
		 */

		Dataset<Row> dataframe2 = session.read()
				.csv("..\\spark_T\\src\\main\\java\\com\\project\\spark_T\\resources\\data2.csv");

		dataframe2.write().option("path", "..\\spark_T\\src\\main\\java\\com\\project\\spark_T\\resources\\table")
				.mode(SaveMode.Append).saveAsTable("Test_Table");

		Encoder<Movie> encoder_movie = Encoders.bean(Movie.class);

		Dataset<Row> dataset = session.read()
				.csv("..\\spark_T\\src\\main\\java\\com\\project\\spark_T\\resources\\data2.txt")
				.toDF("name", "rating", "timestamp");

		Dataset<Row> dataset2 = dataset.select(col("name"), col("rating").cast("double"), col("timestamp"));

		Dataset<Movie> dataset3 = dataset2.as(encoder_movie);

		dataset3.sortWithinPartitions("rating").show();

		dataset3.sort("name").show();

		dataset3.sort(col("rating").desc()).show();

		dataset3.sort(col("rating").asc()).show();

		dataset3.orderBy(col("rating").desc()).show();

		dataset.selectExpr("name", "rating as movie_rating").show();

		dataset3.groupBy(col("name")).avg("rating").show();

		dataset2.dropDuplicates("name").show();

		Tuple2<String, String>[] tup = dataset2.dtypes();

		for (Tuple2<String, String> tuple2 : tup) {

			System.out.println(tuple2._1);

			System.out.println(tuple2._2);

		}

		dataset2.na().drop().show();
	}

}
