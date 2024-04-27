package de.tum.ddm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

public class PrimeNumbersMain {

    private static final Logger LOG = Logger.getLogger(PrimeNumbersMain.class);
    public static final String MAX_NUMBER = "MaxNumber";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] jobArgs = optionParser.getRemainingArgs();
        if (jobArgs.length != 3) {
            LOG.error("Usage: hadoop jar MapReducePrimeNumbers-1.0.jar PrimeNumbersMain <in> <out> <max_number>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "MapReducePrimeNumbers");
        job.setJarByClass(PrimeNumbersMain.class);
        job.setMapperClass(PrimeNumbersMapper.class);
        job.setCombinerClass(PrimeNumbersCombiner.class);
        job.setReducerClass(PrimeNumbersReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.getConfiguration().setInt(MAX_NUMBER, Integer.parseInt(jobArgs[2]));
        FileInputFormat.addInputPath(job, new Path(jobArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(jobArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}