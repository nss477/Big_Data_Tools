import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class invertedIndexReducer 
    extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // Use Set to remove duplicates
        Set<String> documentSet = new TreeSet<>();

        for (Text val : values) {
            documentSet.add(val.toString());
        }

        // Convert Set to comma-separated string
        StringBuilder docList = new StringBuilder();

        for (String doc : documentSet) {
            docList.append(doc).append(", ");
        }

        // Remove last comma and space
        if (docList.length() > 0) {
            docList.setLength(docList.length() - 2);
        }

        result.set(docList.toString());

        // Final Output: word → Doc1.txt, Doc2.txt
        context.write(key, result);
    }
}
