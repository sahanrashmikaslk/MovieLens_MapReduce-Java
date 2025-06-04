package movielens;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Driver (main) to run the MostRated MapReduce job (movie rating counts).
 * Usage: hadoop jar MovieLensAnalysis.jar movielens.MostRatedDriver <input_path> <output_path>
 */
public class MostRatedDriver {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: MostRatedDriver <input_path> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Most Rated Movies Count");
        job.setJarByClass(MostRatedDriver.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(MostRatedMapper.class);
        // Use reducer as combiner to optimize counting (since summing is commutative and associative)
        job.setCombinerClass(MostRatedReducer.class);
        job.setReducerClass(MostRatedReducer.class);

        // Set output key and value types
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Run the job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
