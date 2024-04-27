package de.tum.ddm;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import static de.tum.ddm.WordSearchMain.SEARCH_WORD;

public class WordSearchMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final LongWritable ONE = new LongWritable(1L);
    private static final Pattern PATTEN = Pattern.compile("[^a-z]");
    private static final int PREFIX_LENGTH = "  \"text\": \"".length();
    private static final StringBuilder incompleteJsonText = new StringBuilder();
    private final Text word = new Text();

    private String searchWord;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        searchWord = context.getConfiguration().get(SEARCH_WORD);
        word.set(searchWord);
        super.setup(context);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().toLowerCase();
        if (line.equals(" },") || line.equals(" }")) {
            String text = incompleteJsonText.substring(PREFIX_LENGTH);
            incompleteJsonText.setLength(0);
            line = PATTEN.matcher(text).replaceAll(" ");
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                String currentWord = itr.nextToken();
                if (searchWord.equals(currentWord)) {
                    context.write(word, ONE);
                    return;
                }
            }
        } else if (!line.equals("[")
                && !line.equals("]")
                && !line.equals(" {")
                && !line.startsWith("  \"id\": \"")
                && !line.startsWith("  \"title\": ")) {
            incompleteJsonText.append(line);
        }
    }
}