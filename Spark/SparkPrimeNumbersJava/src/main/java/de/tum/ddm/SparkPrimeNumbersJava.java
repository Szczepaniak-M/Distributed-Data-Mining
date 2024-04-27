package de.tum.ddm;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.reflect.ClassTag;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.spark.sql.functions.col;


public class SparkPrimeNumbersJava {

    private static final Logger LOG = Logger.getLogger(SparkPrimeNumbersJava.class);

    public static void main(String[] args) {

        if (args.length != 3) {
            LOG.error("Usage: spark-submit SparkPrimeNumbersJava-1.0.jar <in> <out> <max_number>");
            System.exit(2);
        }

        List<Integer> divisors = sieveOfEratosthenes(Integer.parseInt(args[2]));

        SparkSession spark = SparkSession.builder()
                .appName("SparkPrimeNumbersJava")
                .getOrCreate();

        Broadcast<List> divisorsBroadcast = spark.sparkContext().broadcast(divisors, classTag(List.class));

        var result = spark.read()
                .textFile(args[0])
                .map((MapFunction<String, Integer>) Integer::valueOf, Encoders.INT())
                .filter((FilterFunction<Integer>) number -> isPrime(number, divisorsBroadcast.value()))
                .orderBy(col("value").desc())
                .limit(100);

        result.write().mode("overwrite").csv(args[1]);
    }

    private static List<Integer> sieveOfEratosthenes(int number) {
        int maxNumber = (int) Math.sqrt(number) + 1;
        List<Integer> primes = new LinkedList<>();
        List<Integer> primeCandidates = IntStream.range(3, maxNumber)
                .filter(num -> (num & 1) != 0)
                .boxed()
                .collect(Collectors.toList());
        int i = 0;
        while (i <= Math.sqrt(maxNumber)) {
            i = primeCandidates.get(0);
            primes.add(i);
            int finalI = i;
            primeCandidates = primeCandidates.stream()
                    .filter(num -> num % finalI != 0)
                    .collect(Collectors.toList());
        }
        primes.addAll(primeCandidates);
        return primes;
    }

    private static <T> ClassTag<T> classTag(Class<T> clazz) {
        return scala.reflect.ClassManifestFactory.fromClass(clazz);
    }

    private static boolean isPrime(int number, List<Integer> divisors) {
        if (number < 2) {
            return false;
        } else if (number == 2) {
            return true;
        } else if ((number & 1) == 0) {
            return false;
        } else {
            return divisors.stream()
                    .filter(div -> number > div)
                    .noneMatch(div -> number % div == 0);
        }
    }
}

