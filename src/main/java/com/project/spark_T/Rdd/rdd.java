package com.project.spark_T.Rdd;

import java.util.Arrays;
import java.util.Iterator;
import org.apache.commons.lang3.StringUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.DoubleFunction;

import com.project.spark_T.model.Average;

import scala.Tuple2;

public class rdd {

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("test").setMaster("local");

		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		JavaRDD<String> radiusData = jsc.textFile("..\\spark_T\\src\\main\\java\\com\\project\\spark_T\\resources\\data.txt");

		JavaRDD<String> filtered2 = radiusData.filter(new Function<String, Boolean>() {

			public Boolean call(String line) throws Exception {

				String[] split = line.split(",");

				if (split[2].contains("port=13")) {
					return true;

				} else {

					return false;
				}
			}
		});

		filtered2.foreach(x -> System.out.println("work" + x));

		/*
		 * 
		 * 
		 * use Function map()
		 * 
		 */

		JavaRDD<String> rdd = jsc.textFile("..\\spark_T\\src\\main\\java\\\\com\\project\\spark_T\\resources\\data1.txt");

		JavaRDD<Double> rdd2 = rdd.map((String x) -> {

			String[] split = x.split(",");

			if (split[4].length() > 0) {

				split[4] = String.valueOf(Double.parseDouble(split[4]) * 2);

				return Double.parseDouble(split[4]);

			}
			return 0d;
		});

		for (Double string : rdd2.collect()) {

			System.out.println(string.toString());

		}

		/*
		 * 
		 * use Function flatMap()
		 * 
		 */

		JavaRDD<String> word = rdd.flatMap(x -> {

			String[] eachElement = x.split(",");

			return (Iterator<String>) Arrays.asList(eachElement).iterator();

		});

		for (String string : word.collect()) {

			System.out.println("letze" + string);

		}

		/*
		 * 
		 * use Function reduce()
		 * 
		 * 
		 */

		String sum = rdd.reduce((x, y) -> {

			String[] one = x.split(",");

			String[] two = y.split(",");

			if (one[4].length() > 0 && two[4].length() > 0) {

				one[4] = String.valueOf(Double.parseDouble(one[4]) + Double.parseDouble(two[4]));

			} else if (two[4].length() > 0) {
				one[4] = two[4];
			}

			return StringUtils.join(one, ",");
		});

		/*
		 * 
		 * 
		 * use Function mapToPair()
		 * 
		 * 
		 */

		JavaPairRDD<String, String> pairs = rdd.mapToPair(new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String x) throws Exception {

				return new Tuple2<String, String>(x.split(",")[0], x);

			}
		});

		JavaPairRDD<String, String> filteredRdd = pairs.filter(x -> {

			if (Integer.parseInt(x._2.split(",")[4]) > 0) {

				return true;
			} else {

				return false;
			}
		});

		for (Tuple2 string : filteredRdd.collect()) {

			System.out.println("Key " + string._1 + " Value " + string._2);
		}

		/*
		 * 
		 * 
		 * use Function combineByKey()
		 * 
		 * 
		 */

		JavaRDD<String> rdd3 = jsc.textFile("..\\spark_T\\src\\main\\java\\com\\project\\spark_T\\resources\\data2.txt");

		JavaPairRDD<String, Double> pairRdd = rdd3.mapToPair(new PairFunction<String, String, Double>() {

			@Override
			public Tuple2<String, Double> call(String str) throws Exception {

				String[] data = str.split(",");

				return new Tuple2<String, Double>(data[0], Double.parseDouble(data[1]));

			}
		});

		Function<Double, Average> combiner = new Function<Double, Average>() {

			@Override
			public Average call(Double value) throws Exception {

				return new Average(1, value);

			}

		};

		Function2<Average, Double, Average> mergeValue = new Function2<Average, Double, Average>() {

			@Override
			public Average call(Average avg, Double value) throws Exception {

				avg.setSum(avg.getSum() + value);
				avg.setCount(avg.getCount() + 1);

				return avg;
			}
		};

		Function2<Average, Average, Average> mergeCombiners = new Function2<Average, Average, Average>() {

			@Override
			public Average call(Average avg1, Average avg2) throws Exception {

				avg1.setCount(avg1.getCount() + avg2.getCount());
				avg1.setSum(avg1.getSum() + avg2.getSum());

				return avg1;

			}
		};

		JavaPairRDD<String, Average> output = pairRdd.combineByKey(combiner, mergeValue, mergeCombiners);

		for (Tuple2<String, Average> string : output.collect()) {

			System.out.println(string._1 + " " + string._2.average());

		}

		/*
		 * 
		 * 
		 * use Function groupBy()
		 * 
		 * 
		 */

		JavaPairRDD<String, Iterable<String>> group = rdd3.groupBy(new Function<String, String>() {

			@Override
			public String call(String arg0) throws Exception {

				String[] data = arg0.split(",");

				return data[0];

			}
		});
		for (Tuple2<String, Iterable<String>> string : group.collect()) {

			Iterable<String> it = string._2;
			Iterator<String> iterator = it.iterator();

			int count = 0;
			double sum1 = 0;
			while (iterator.hasNext()) {
				sum1 += Double.parseDouble(iterator.next().split(",")[1]);

				count++;
			}

			System.out.println("Average" + string._1 + " - " + sum1 / count);

		}

		/*
		 * 
		 * 
		 * use inner join
		 * 
		 * 
		 */

		JavaRDD<String> rdd4 = jsc.textFile("..\\spark_T\\src\\main\\java\\com\\project\\spark_T\\resources\\DateSet two.txt");

		JavaRDD<String> rdd5 = jsc.textFile("..\\spark_T\\src\\main\\java\\com\\project\\spark_T\\resources\\DataSet One.txt");

		JavaPairRDD<String, Double> pairRdd4 = rdd4.mapToPair(new PairFunction<String, String, Double>() {

			@Override
			public Tuple2<String, Double> call(String str) throws Exception {

				String[] data = str.split(",");
				return new Tuple2<String, Double>(data[0], Double.parseDouble(data[1]));

			}
		});
		for (Tuple2 string : pairRdd4.collect()) {

			System.out.println("ta " + string._1 + " Value " + string._2);
		}

		JavaPairRDD<String, String> pairRdd5 = rdd5.mapToPair(new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String str) throws Exception {

				String[] data = str.split(",", -1);
				return new Tuple2<String, String>(data[1], data[2]);

			}
		});
		for (Tuple2 string : pairRdd5.collect()) {

			System.out.println("ya " + string._1 + " Value " + string._2);
		}

		JavaPairRDD<String, Tuple2<Double, String>> innerRdd = pairRdd4.join(pairRdd5);

		for (Tuple2<String, Tuple2<Double, String>> tu : innerRdd.collect()) {

			System.out.println(tu._1 + " " + tu._2._1 + " " + tu._2._2);

		}

		/*
		 * 
		 * Statistics
		 * 
		 */
		JavaDoubleRDD pair = rdd4.mapToDouble(new DoubleFunction<String>() {
			@Override
			public double call(String arg0) throws Exception {

				return Double.parseDouble(arg0.split(",")[1]);
			}
		});

		System.out.println(pair.stats().count());
		System.out.println(pair.stats().mean());
		System.out.println(pair.stats().sum());
		System.out.println(pair.stats().max());
		System.out.println(pair.stats().min());
		System.out.println(pair.stats().variance());
		System.out.println(pair.stats().sampleStdev());
		System.out.println(pair.stats().sampleVariance());
		System.out.println(pair.stats().stdev());
		System.out.println(pair.max());

	}

}
