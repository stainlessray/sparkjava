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
     * @param chaptersRdd = Pair RDD containing all data from chapters csv file
     * returns: new (sorted) rdd containing a count of number of chapters associated with each courseId
     * **/
    public static JavaPairRDD<Integer, Integer> createChapterCountPerCourseRdd(JavaPairRDD<Integer, Integer> chaptersRdd) {
        return chaptersRdd
                .mapToPair( item -> new Tuple2<>( item._2, 1))
                .reduceByKey((item1, item2) -> item1+item2)
                .sortByKey();
    }

    /**
     * desc: return an rdd with each userId as key and each chapterId they viewed in distinct() format (no duplicate chapters listed)
     * @param individualChaptersViewedRdd Pair RDD named "viewsRdd" containing a pair of integer values
     * returns: new (sorted) rdd containing userId as key and each chapterId they viewed in distinct() format
     * **/
    public static JavaPairRDD<Integer, Integer> removeDuplicateChapterViews(JavaPairRDD<Integer, Integer> individualChaptersViewedRdd) {
        return individualChaptersViewedRdd
                .distinct()
                .sortByKey();
    }
}
