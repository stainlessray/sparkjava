package com.virtualpairprogrammers.section12;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.apache.log4j.Level.WARN;

public class Joins {

    private static final Logger logger = LogManager.getLogger("org.apache");

    public static void main(String[] args) {
        logger.setLevel(WARN);

        List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
        visitsRaw.add(new Tuple2<>(4,18));
        visitsRaw.add(new Tuple2<>(6,4));
        visitsRaw.add(new Tuple2<>(10,9));

        List<Tuple2<Integer, String>> usersRaw = new ArrayList<>();
        usersRaw.add(new Tuple2<>(1, "John"));
        usersRaw.add(new Tuple2<>(2, "Bob"));
        usersRaw.add(new Tuple2<>(3, "Alan"));
        usersRaw.add(new Tuple2<>(4, "Doris"));
        usersRaw.add(new Tuple2<>(5, "Marybelle"));
        usersRaw.add(new Tuple2<>(6, "Raquel"));

        SparkConf conf = new SparkConf()
                .setAppName("startingSpark")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitsRaw);
        JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);



        JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> rightOuterJoinedRdd = visits.rightOuterJoin(users);
        System.out.println("Printing Right joined tables");
        rightOuterJoinedRdd.collect().forEach(it -> System
                .out.println("User, " + it._2._2 + " visited " + it._2._1.orElse(0) + " times"));

        System.out.println("\n");

        JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> leftJoinedRdd = visits.leftOuterJoin(users);
        System.out.println("Printing Left joined tables ");
        leftJoinedRdd.collect().forEach(it -> System
                .out.println(it._2._2
                        .orElse("(empty)")
                        .toUpperCase(Locale.ROOT)));

        System.out.println("\n");


        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> cartesianJoinedRdd = visits.cartesian(users);
        cartesianJoinedRdd.collect().forEach(System.out::println);

        sc.close();
    }
}
