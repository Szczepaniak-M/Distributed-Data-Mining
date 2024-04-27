package de.tum.ddm;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.PriorityQueue;

public class PrimeNumbersReducer extends Reducer<IntWritable, IntWritable, Text, Text> {

    private final PriorityQueue<Integer> primeNumbersTop = new PriorityQueue<>();

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
            context.write(new Text(prime.toString()), new Text());
        }
    }
}