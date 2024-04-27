package de.tum.ddm;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.PriorityQueue;

public class PrimeNumbersCombiner extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private final PriorityQueue<Integer> primeNumbersTop = new PriorityQueue<>();

    private static final IntWritable one = new IntWritable(1);

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> primeNumbers, Context context) throws IOException, InterruptedException {
        for (IntWritable prime : primeNumbers) {
            if (primeNumbersTop.size() < 100 || primeNumbersTop.peek() < prime.get()) {
                primeNumbersTop.add(prime.get());
            }
            if (primeNumbersTop.size() > 100) {
                primeNumbersTop.poll();
            }
        }
        for (Integer prime : primeNumbersTop) {
            context.write(one, new IntWritable(prime));
        }
    }
}
