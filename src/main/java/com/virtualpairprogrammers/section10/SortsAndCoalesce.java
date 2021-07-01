package com.virtualpairprogrammers.section10;

import com.virtualpairprogrammers.Util;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

import static org.apache.log4j.Level.WARN;

public class SortsAndCoalesce {
    private static final Logger logger = LogManager.getLogger("org.apache");

    public static void main(String[] args) {
        logger.setLevel(WARN);

        SparkConf conf = new SparkConf()
                .setAppName("startingSpark")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");

        JavaRDD<String> lettersOnly = initialRdd
                .map( sentence -> sentence.replaceAll( "[^a-zA-Z\\s]", "" )
                        .toLowerCase()
                        .trim()
                );

        JavaRDD<String> removedBlankLines = lettersOnly
                .filter( sentence -> sentence
                        .trim()
                        .length() > 0);

        JavaRDD<String> justWords = removedBlankLines
                .flatMap( sentence -> Arrays.asList(sentence.split(" ")).iterator())
                .filter( word -> word.trim().length() > 0 )
                .filter(word -> Util.isNotBoring(word));


         /*       JavaRDD<String> blankWordsRemoved = justWords.filter( word -> word.trim().length() > 0 );
        JavaRDD<String> justInterestingWords = blankWordsRemoved
                .filter(word -> Util.isNotBoring(word));*/


        JavaPairRDD<String, Long> pairedAndCounted = justWords
                .mapToPair(word -> new Tuple2<>(word, 1L))
                .reduceByKey(( value1, value2 ) -> value1 + value2);


        //JavaPairRDD<String, Long> totals = pairRdd.reduceByKey(( value1, value2 ) -> value1 + value2);


        JavaPairRDD<Long, String> reversedKeysAndValues = pairedAndCounted
                .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));

        JavaPairRDD<Long, String> sorted = reversedKeysAndValues.sortByKey(false);


        //sorted.foreach(element -> System.out.println(element));


        List<Tuple2<Long, String>> results = sorted.collect();

       // List<Tuple2<Long, String>> results = sorted.take(50);
        results.forEach(System.out::println);

        sc.close();

    }
}
