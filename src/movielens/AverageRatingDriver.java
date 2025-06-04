package movielens;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Driver (main) to run the AverageRating MapReduce job.
 * Usage: hadoop jar MovieLensAnalysis.jar movielens.AverageRatingDriver <input_path> <output_path>
 */
public class AverageRatingDriver {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: AverageRatingDriver <input_path> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Rating per Movie");
        job.setJarByClass(AverageRatingDriver.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(AverageRatingMapper.class);
        job.setReducerClass(AverageRatingReducer.class);
        // (Optionally, set Combiner as AverageRatingReducer if partial aggregation needed)

        // Set output key and value types (for both mapper and reducer)
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Set input and output paths from arguments
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit the job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
