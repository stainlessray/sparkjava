package com.virtualpairprogrammers.sections_one_thru_eight;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.log4j.Level.WARN;

public class MapsAndFilters {
    private static final Logger logger = LogManager.getLogger("org.apache");

    public static void main(String[] args) {
        List<String> inputSData = new ArrayList<>();
        inputSData.add("Warn: Tuesday 4 September 0405");
        inputSData.add("Error: Tuesday 4 September 0408");
        inputSData.add("Fatal: Wednesday 5 September 1632");
        inputSData.add("Error: Friday 7 September 1854");
        inputSData.add("Warn: Saturday 8 September 1942");


        logger.setLevel(WARN);
        SparkConf conf = new SparkConf()
                .setAppName("startingSpark")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.parallelize(inputSData)
                .flatMap( value -> Arrays.asList( value.split(" ")).iterator())
                .filter( word -> word.length() > 1)
                .collect()
                .forEach(System.out::println);


/*        JavaRDD<String> words = sentences.flatMap( value -> Arrays.asList( value.split(" ")).iterator());

        JavaRDD<String> filteredWords = words.filter( word -> word.length() > 1);

        filteredWords.collect().forEach(System.out::println);*/
    }
}
