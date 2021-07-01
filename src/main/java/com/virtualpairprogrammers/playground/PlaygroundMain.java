package com.virtualpairprogrammers.playground;

import com.google.common.collect.Iterables;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
//import java.util.Arrays;
import java.util.List;

//import static java.lang.System.*;
import static org.apache.log4j.Level.WARN;

/**
 * Use this class to create
 * secondary practice playground
 * where you can experiment with
 * the current concepts outside
 * the scope of the current lesson
 * **/


public class PlaygroundMain {
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


        /*
        * reduceByKey() is preferred over groupByKey() for stability reasons
        * in particular with larger data sets
        * */
        sc.parallelize(inputSData)
                .mapToPair( rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L ))
                .reduceByKey( (value1, value2) -> value1 + value2 )
                .foreach(tuple -> System.out.println( tuple._1 + " count = " + tuple._2));

        /*
          groupByKey() is not a great way to count values in big data. Better to use reduceBy()
          but it works
          **/
        sc.parallelize(inputSData)
                .mapToPair( rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L ))
                .groupByKey()
                .foreach( tuple -> System.out.println( tuple._1 + " count = " + Iterables.size( tuple._2) ));

        sc.close();
    }

}
