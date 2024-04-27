package de.tum.ddm;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final LongWritable ONE = new LongWritable(1L);
    private static final Pattern PATTEN = Pattern.compile("[^a-z]");
    private final Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().toLowerCase();
        line = PATTEN.matcher(line).replaceAll(" ");
        StringTokenizer itr = new StringTokenizer(line);
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, ONE);
        }
    }
}