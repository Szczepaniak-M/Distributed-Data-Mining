package de.tum.ddm;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.reflect.ClassTag;

import java.util.Arrays;
import java.util.regex.Pattern;

public class SparkWordCountJava {

    private static final Logger LOG = Logger.getLogger(SparkWordCountJava.class);
    private static final Pattern PATTERN = Pattern.compile("[^a-z]");

    public static void main(String[] args) {

        if (args.length != 2) {
            LOG.error("Usage: spark-submit SparkWordCountJava-1.0.jar <inputFile> <outputFolder>");
            System.exit(2);
        }

        SparkSession spark = SparkSession.builder()
                .appName("SparkWordCountJava")
                .getOrCreate();

        Broadcast<Pattern> pattern = spark.sparkContext().broadcast(PATTERN, classTag(Pattern.class));

        Dataset<Row> result = spark.read()
                .textFile(args[0])
                .map((MapFunction<String, String>) String::toLowerCase, Encoders.STRING())
                .map((MapFunction<String, String>) line -> pattern.value().matcher(line).replaceAll(" "), Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) line -> Arrays.stream(line.split(" ")).iterator(), Encoders.STRING())
                .filter((FilterFunction<String>) word -> !word.isEmpty())
                .groupBy("value")
                .count();

        result.write().mode("overwrite").csv(args[1]);
    }

    private static <T> ClassTag<T> classTag(Class<T> clazz) {
        return scala.reflect.ClassManifestFactory.fromClass(clazz);
    }
}
