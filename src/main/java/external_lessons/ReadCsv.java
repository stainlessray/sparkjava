package external_lessons;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


import static org.apache.log4j.Level.WARN;

public class ReadCsv {

    private static final Logger logger = LogManager.getLogger("org.apache");
    private static final String file = "src/main/resources/external_lesson_resources/NYPD_7_Major_Felony_Incidents.csv";

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        logger.setLevel(WARN);

        SparkConf conf = new SparkConf()
                .setAppName("startingSpark")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> data = sc.textFile(file);

        String header = data.first();
        System.out.println(header);
        data.foreach(System.out::println);


        sc.close();

    }
}