package com.virtualpairprogrammers.section12;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.List;

import static org.apache.log4j.Level.WARN;

public class JoinPractice {

    private static final Logger logger = LogManager.getLogger("org.apache");

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        logger.setLevel(WARN);

        SparkConf conf = new SparkConf()
                .setAppName("reduce_practice")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<Integer, Integer> viewsTable = sc.textFile("src/main/resources/viewing figures/views-*.csv")
                .mapToPair(item -> {
                    String[] columns = item.split(",");
                    return new Tuple2<>(new Integer(columns[0]), new Integer(columns[1]));
                });


        // create an rdd containing a userId and each distinct view of a chapter. (removes duplicate chapter views by the userId)
        JavaPairRDD<Integer, Integer> viewsTableDistinctPerUser = viewsTable.distinct().sortByKey();
        viewsTableDistinctPerUser.collect().forEach(System.out::println);

        JavaPairRDD<Integer, Integer> chaptersTable = sc.textFile("src/main/resources/viewing figures/chapters.csv")
                .mapToPair(item -> {
                    String[] columns = item.split(",");
                    return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
                });


        // create rdd with the number of chapters per courseId
        JavaPairRDD<Integer, Integer> chaptersPerCourse = createChapterCountPerCourseRdd(chaptersTable);
        //chaptersPerCourse.collect().forEach(System.out::println);


        // rdd with chapter and count of course id's (each chapter should only correlate to one course id)
        // This produces a list of distinct chapters and confirms that each chapter is only associated with one course
        JavaPairRDD<Integer, Integer> chapterCountsRdd = createCountOfCoursesPerChapterRdd(chaptersTable);
        //chapterCountsRdd.collect().forEach(System.out::println);

        // left join viewsTable > chapterTable
        // Contains userId, and tuple<chapterId, optional<courseId>
        JavaPairRDD<Integer, Tuple2<Integer, Optional<Integer>>> leftJoinedRdd = viewsTable.leftOuterJoin(chaptersTable);
        //leftJoinedRdd.collect().forEach(System.out::println);

/*        List<Tuple2<Integer, Tuple2<Integer, Optional<Integer>>>> leftJoinedTable = leftJoinedRdd.take(50);
        leftJoinedTable.forEach(System.out::println);*/
    }

    private static JavaPairRDD<Integer, Integer> createCountOfCoursesPerChapterRdd(JavaPairRDD<Integer, Integer> chaptersTable) {
        JavaPairRDD<Integer, Integer> chapterCountsRdd = chaptersTable
                .mapToPair(item -> new Tuple2<>( item._1, 1 ))
                .reduceByKey((value1, value2) -> value1+value2)
                .sortByKey();
        return chapterCountsRdd;
    }

    private static JavaPairRDD<Integer, Integer> createChapterCountPerCourseRdd(JavaPairRDD<Integer, Integer> chaptersTable) {
        JavaPairRDD<Integer, Integer> chaptersPerCourse = chaptersTable
                .mapToPair( item -> new Tuple2<>( item._2, 1))      // make each courseId a key and assign value of one to each listing of the course
                .reduceByKey((item1, item2) -> item1+item2)         // count the number of times the course id is listed by summing the 1's
                .sortByKey();                                       // sort by courseId
        return chaptersPerCourse;
    }
}
