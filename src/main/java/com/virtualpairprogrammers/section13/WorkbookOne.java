package com.virtualpairprogrammers.section13;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import static org.apache.log4j.Level.WARN;

public class WorkbookOne {
    private static final Logger logger = LogManager.getLogger("org.apache");
    private static final String viewsFilePath = "src/main/resources/viewing figures/views-*.csv";
    private static final String chaptersFilePath = "src/main/resources/viewing figures/chapters.csv";
    private static final String titlesFilePath = "src/main/resources/viewing figures/titles.csv";

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        logger.setLevel(WARN);

        SparkConf conf = new SparkConf()
                .setAppName("reduce_practice")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<Integer, Integer> viewsRdd = sc.textFile(viewsFilePath)
                .mapToPair(row -> {
                    String[] columns = row.split(",");
                    return new Tuple2<>(new Integer(columns[0]), new Integer(columns[1]));
                });

        JavaPairRDD<Integer, Integer> chaptersRdd = sc.textFile(chaptersFilePath)
                .mapToPair(row -> {
                    String[] columns = row.split(",");
                    return new Tuple2<>(new Integer(columns[0]), new Integer(columns[1]));
                });

        JavaPairRDD<Integer, String> titles = sc.textFile(titlesFilePath)
                .mapToPair(row -> {
                    String[] columns = row.split(",");
                    return new Tuple2<>(new Integer(columns[0]), (columns[1]));
                });

        JavaPairRDD<Integer, Integer> chaptersPerCourseRdd = chaptersRdd.mapToPair(item -> new Tuple2<>(item._2, 1))
                .reduceByKey(Integer::sum)
                .sortByKey();

        JavaPairRDD<Integer, Tuple2<Long, String>> finalRdd = viewsRdd
                .mapToPair(item -> new Tuple2<>(item._2, item._1))
                .distinct()
                .join(chaptersRdd)
                .mapToPair(item -> new Tuple2<>(new Tuple2<>(item._2._1(), item._2._2()), 1))
                .reduceByKey(Integer::sum)
                .mapToPair(item -> new Tuple2<>(item._1._2(), item._2))
                .join(chaptersPerCourseRdd)
                .mapValues(value -> (double) value._1 / value._2)
                .mapValues(value -> {
                    if (value > 0.9) return 10L;
                    if (value > 0.5) return 4L;
                    if (value > 0.25) return 2L;
                    return 0L;
                })
                .reduceByKey(Long::sum)
                .join(titles)
                .sortByKey();

        finalRdd.collect().forEach(item -> System.out.println(
                "Course Summary: \n" +
                        " Course ID: " + item._1 +
                        "\n Course name: " + item._2._2() +
                        "\n Score: " + item._2._1() + "\n"));

    }
}

