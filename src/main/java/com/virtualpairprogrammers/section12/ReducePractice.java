package com.virtualpairprogrammers.section12;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import static org.apache.log4j.Level.WARN;

public class ReducePractice {

    private static final Logger logger = LogManager.getLogger("org.apache");

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        logger.setLevel(WARN);

        SparkConf conf = new SparkConf()
                .setAppName("reduce_practice")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<Tuple3<Integer, Integer, String>> rawViewsData = sc.textFile("src/main/resources/viewing figures/views-*.csv")
                .map(item -> {
                    java.lang.String[] columns = item.split(",");
                    return new Tuple3<>(new Integer(columns[0]), new Integer(columns[1]), columns[2]);
                });

        JavaRDD<Integer> courseId = rawViewsData.map(Tuple3::_2);

        JavaPairRDD<Integer, Integer> pairedReduced = courseId
                .mapToPair(item -> new Tuple2<>(item, 1))
                .reduceByKey(Integer::sum)
                .mapToPair(Tuple2::swap)
                .sortByKey()
                .mapToPair(item -> item.swap());
        pairedReduced.collect().forEach(item -> System.out.println(" Chapter ID: " + item._1 + " Total Views: "  + item._2));

        /*
          More explicitly executed below. The above reference is for cleaner
          more functional look and performant
          **/
        JavaRDD<Integer> courseIdEx = rawViewsData.map(item -> item._2());

        JavaPairRDD<Integer, Integer> pairedReducedEx = courseIdEx
                .mapToPair(item -> new Tuple2<>(item, 1))
                .reduceByKey((item1, item2) -> item1 + item2)
                .mapToPair(pair -> pair.swap())
                .sortByKey()
                .mapToPair(item -> item.swap());

        pairedReducedEx.collect().forEach(item -> System.out.println(" Chapter ID: " + item._1 + " Total Views: "  + item._2));

        sc.close();
    }
}

