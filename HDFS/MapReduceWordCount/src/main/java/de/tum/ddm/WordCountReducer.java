package de.tum.ddm;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private final LongWritable result = new LongWritable();

    @Override
    public void reduce(Text word, Iterable<LongWritable> counts, Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable count : counts) {
            sum += count.get();
        }
        result.set(sum);
        context.write(word, result);
    }
}