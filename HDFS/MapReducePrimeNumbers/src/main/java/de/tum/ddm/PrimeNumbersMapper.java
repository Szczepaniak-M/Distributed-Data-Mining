package de.tum.ddm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static de.tum.ddm.PrimeNumbersMain.MAX_NUMBER;

public class PrimeNumbersMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private static final IntWritable one = new IntWritable(1);

    private final List<Integer> divisors = new LinkedList<>();

    @Override
    public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        int maxNumber = (int) Math.sqrt(conf.getLong(MAX_NUMBER, 1_000_000_000)) + 1;
        List<Integer> primeCandidates = IntStream.range(3, maxNumber)
                .filter(num -> (num & 1) != 0)
                .boxed()
                .collect(Collectors.toList());
        int i = 0;
        while (i <= Math.sqrt(maxNumber)) {
            i = primeCandidates.get(0);
            divisors.add(i);
            int finalI = i;
            primeCandidates = primeCandidates.stream()
                    .filter(num -> num % finalI != 0)
                    .collect(Collectors.toList());
        }
        divisors.addAll(primeCandidates);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int number = Integer.parseInt(value.toString());
        if (number == 2) {
            context.write(one, new IntWritable(number));
        } else if ((number & 1) != 0) {
            boolean isPrime = divisors.stream()
                    .filter(divisor -> number > divisor)
                    .noneMatch(divisor -> number % divisor == 0);
            if (isPrime) {
                context.write(one, new IntWritable(number));
            }
        }
    }
}