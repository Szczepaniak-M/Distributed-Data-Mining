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

public class WordSearchMain {

    private static final Logger LOG = Logger.getLogger(WordSearchMain.class);

    public static final String SEARCH_WORD = "search-word";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] jobArgs = optionParser.getRemainingArgs();
        if ((jobArgs.length != 3)) {
            LOG.error("Usage: hadoop jar MapReduceWordSearch-1.0.jar de.tum.ddm.WordSearchMain <in> <out> <search_word>");
            System.exit(2);
        }


        Job job = Job.getInstance(conf, "MapReduceWordSearch");
        job.setJarByClass(WordSearchMain.class);
        job.setMapperClass(WordSearchMapper.class);
        job.setCombinerClass(WordSearchReducer.class);
        job.setReducerClass(WordSearchReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(NonSplitableFileInputFormat.class);
        job.getConfiguration().set(SEARCH_WORD, args[2]);
        FileInputFormat.addInputPath(job, new Path(jobArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(jobArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}