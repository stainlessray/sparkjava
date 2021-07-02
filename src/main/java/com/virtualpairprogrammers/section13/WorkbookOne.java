package com.virtualpairprogrammers.section13;

import com.virtualpairprogrammers.BasicUtilities;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import static org.apache.log4j.Level.WARN;

public class WorkbookOne {
    private static final Logger logger = LogManager.getLogger("org.apache");
    private static final String viewsFilePath = "src/main/resources/viewing figures/views-*.csv";
    private static final String chaptersFilePath = "src/main/resources/viewing figures/chapters.csv";
    private static final int rowCount = 5000;

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
        //todo join individualChaptersViewedRdd with chaptersRdd
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedRdd = individualChaptersViewedRdd.join(chaptersRdd);



        JavaPairRDD<Tuple2<Integer, Integer>, Integer> droppedChapterId = joinedRdd
                .mapToPair(item -> new Tuple2<>( new Tuple2<>(item._2._1(),item._2._2() ), 1))
                .reduceByKey( (item1, item2) -> item1+item2);

        JavaPairRDD<Integer, Integer> droppedUser = droppedChapterId
                .mapToPair(item -> new Tuple2<>(item._1._2(), item._2));
       // droppedUser.collect().forEach(System.out::println);


        JavaPairRDD<Integer, Tuple2<Integer, Integer>> chaptersViewedOfChaptersAvailable = droppedUser.join(chaptersPerCourseRdd);

        JavaPairRDD<Integer, Double> percentages = chaptersViewedOfChaptersAvailable
                .mapValues( value -> (double)value._1 / value._2);

        percentages.collect().forEach(System.out::println);



/* If a user watches more than 90% of the course, the course gets 10 points
 If a user watches > 50% but <90% , it scores 4
 If a user watches > 25% but < 50% it scores 2
 Less than 25% is no score */

/*        droppedChapterId.collect().forEach(System.out::println);
        droppedChapterId.collect().forEach(item -> System.out.println(String.valueOf(item._1 ) + String.valueOf(+ item._2)));*/
        //BasicUtilities.outputFirstNRows(individualChaptersViewedRdd, rowCount);
    }


    //todo create a utility method that swaps any pair rdd


}

