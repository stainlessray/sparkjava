package com.virtualpairprogrammers.section13;

import com.virtualpairprogrammers.BasicUtilities;
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
    private static final int rowCount = 50;

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

        JavaPairRDD<Integer, Integer> chaptersPerCourseRdd = BasicUtilities.createChapterCountPerCourseRdd(chaptersRdd);
        JavaPairRDD<Integer, Integer> individualChaptersViewedRdd = BasicUtilities.removeDuplicateChapterViews(viewsRdd);
        BasicUtilities.outputFirstNRows(individualChaptersViewedRdd, rowCount);
    }




    //todo create a utility method that swaps any pair rdd


}

