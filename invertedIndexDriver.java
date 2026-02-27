import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class invertedIndexDriver {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: InvertedIndexDriver <input path> <output path>");
            System.exit(-1);
        }

        // Create configuration object
        Configuration conf = new Configuration();

        // Create job
        Job job = Job.getInstance(conf, "Inverted Index");
        job.setJarByClass(invertedIndexDriver.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(invertedIndexMapper.class);
        job.setReducerClass(invertedIndexReducer.class);

        // Set output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
