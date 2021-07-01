package com.virtualpairprogrammers.sections_one_thru_eight;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static org.apache.log4j.Level.WARN;

public class OriginalOne {

    public static void main(String[] args) {
        List<String> inputSData = new ArrayList<>();
        inputSData.add("Warn: Tuesday 4 September 0405");
        inputSData.add("Error: Tuesday 4 September 0408");
        inputSData.add("Fatal: Wednesday 5 September 1632");
        inputSData.add("Error: Friday 7 September 1854");
        inputSData.add("Warn: Saturday 8 September 1942");


        Logger.getLogger("org.apache").setLevel(WARN);
        SparkConf conf = new SparkConf()
                .setAppName("startingSpark")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.parallelize(inputSData)
                .mapToPair( rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L ))
                .reduceByKey( (value1, value2) -> value1 + value2)
                .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2)) ;

        sc.close();
    }
}
