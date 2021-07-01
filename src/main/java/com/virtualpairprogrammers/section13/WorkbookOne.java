package com.virtualpairprogrammers.section13;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

import static org.apache.log4j.Level.WARN;

public class WorkbookOne {
    private static final Logger logger = LogManager.getLogger("org.apache");
    private static final String viewsFilePath = "src/main/resources/viewing figures/views-*.csv";
    private static final String chaptersFilePath = "src/main/resources/viewing figures/chapters.csv";
    private static int rowCount = 5;

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        logger.setLevel(WARN);

        SparkConf conf = new SparkConf()
                .setAppName("reduce_practice")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<Integer, Integer> viewsRdd = sc.textFile(viewsFilePath)
                .mapToPair( row -> {
                    String[] columns = row.split(",");
                    return new Tuple2<>(new Integer(columns[0]), new Integer(columns[1]));
                });

        JavaPairRDD<Integer, Integer> chaptersRdd = sc.textFile(chaptersFilePath)
                .mapToPair( row -> {
                    String[] columns = row.split(",");
                    return new Tuple2<>(new Integer(columns[0]), new Integer(columns[1]));
                });

        JavaPairRDD<Integer, Integer> chaptersPerCourseRdd = createChapterCountPerCourseRdd(chaptersRdd);
        outputFirstNRows(chaptersPerCourseRdd, rowCount);
    }

    //todo create a utility method that swaps any pair rdd

    //todo create a utility method that takes the first n rows from rdd and output to screen
    /**
     * @desc print output sample to screen
     * @param input = Pair RDD containing a pair of Integer values
     * @return null
     *
     * **/
    public static void outputFirstNRows(JavaPairRDD<Integer, Integer> input, int rowCount) {
        List<Tuple2<Integer, Integer>> results = input.take(rowCount);
        results.forEach(System.out::println);
    }

    /**
     * @param chaptersRdd = Pair RDD containing all data from chapters csv file
     * @return new (sorted) rdd containing a count of number of chapters associated with each courseId
     * **/
    private static JavaPairRDD<Integer, Integer> createChapterCountPerCourseRdd(JavaPairRDD<Integer, Integer> chaptersRdd) {
        JavaPairRDD<Integer, Integer> chaptersPerCourseRdd = chaptersRdd
                .mapToPair( item -> new Tuple2<>( item._2, 1))
                .reduceByKey((item1, item2) -> item1+item2)
                .sortByKey();
        return chaptersPerCourseRdd;
    }
}
