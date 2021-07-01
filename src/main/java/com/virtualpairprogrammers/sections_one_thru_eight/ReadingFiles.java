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

public class ReadingFiles {
    private static final Logger logger = LogManager.getLogger("org.apache");

    public static void main(String[] args) {


        logger.setLevel(WARN);
        SparkConf conf = new SparkConf()
                .setAppName("startingSpark")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");


        initialRdd
                .flatMap( value -> Arrays.asList( value.split(" ")).iterator())
                .collect()
                .forEach(System.out::println);

    }
}
