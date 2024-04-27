package de.tum.ddm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

public class WordCountMain {

    private static final Logger LOG = Logger.getLogger(WordCountMain.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] jobArgs = optionParser.getRemainingArgs();
        if ((jobArgs.length != 2)) {
            LOG.error("Usage: hadoop jar MapReduceWordCount-1.0.jar WordCountMain <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "MapReduceWordCount");
        job.setJarByClass(WordCountMain.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(jobArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(jobArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}