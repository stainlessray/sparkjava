package com.virtualpairprogrammers;

import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.List;

public class BasicUtilities {

    /**
     * desc: print output sample to screen
     * @param input = Pair RDD containing a pair of Integer values
     * returns: null
     *
     * **/
    public static void outputFirstNRows(JavaPairRDD<Integer, Integer> input, int rowCount) {
        List<Tuple2<Integer, Integer>> results = input.take(rowCount);
        results.forEach(System.out::println);
    }

    /**
     * @param viewsRdd Pair RDD containing a pair of integer values
     * returns: new rdd containing chapterId as key and each userId who viewed it distinct() format. this requires reversal of the rdd pair
     * **/
    public static JavaPairRDD<Integer, Integer> removeDuplicateChapterViews(JavaPairRDD<Integer, Integer> viewsRdd) {
        return viewsRdd
                .mapToPair( item -> new Tuple2<>(item._2, item._1))
                .distinct();
    }

    /**
     * @param chaptersRdd = Pair RDD containing all data from chapters csv file
     * returns: new (sorted) rdd containing a count of number of chapters associated with each courseId
     * **/
    public static JavaPairRDD<Integer, Integer> createChapterCountPerCourseRdd(JavaPairRDD<Integer, Integer> chaptersRdd) {
        return chaptersRdd
                .mapToPair( item -> new Tuple2<>( item._2, 1))
                .reduceByKey((item1, item2) -> item1+item2)
                .sortByKey();
    }


}
