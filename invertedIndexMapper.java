import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class invertedIndexMapper 
    extends Mapper<LongWritable, Text, Text, Text> {

    private Text word = new Text();
    private Text documentID = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Get file name (Document ID)
        String fileName = ((FileSplit) context.getInputSplit())
                .getPath().getName();
        documentID.set(fileName);

        // Convert line to lowercase
        String line = value.toString().toLowerCase();

        // Remove punctuation and special characters
        line = line.replaceAll("[^a-z0-9\\s]", "");

        // Tokenize line into words
        StringTokenizer tokenizer = new StringTokenizer(line);

        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken().trim();

            if (!token.isEmpty()) {
                word.set(token);
                context.write(word, documentID);
            }
        }
    }
}
