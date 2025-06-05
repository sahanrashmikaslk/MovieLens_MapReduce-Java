package movielens;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper for counting ratings per movie (finding most rated movies).
 * Emits (movieId, 1) for each rating record.
 */

public class MostRatedMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private IntWritable movieIdKey = new IntWritable();
    private final static IntWritable oneValue = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Each line: userId,movieId,rating,timestamp
        String line = value.toString().trim();
        if (line.isEmpty() || line.startsWith("userId")) {
            // Skip empty lines or header
            return;
        }
        String[] fields = line.split(",");
        if (fields.length < 2) {
            return;
        }
        try {
            int movieId = Integer.parseInt(fields[1].trim());
            movieIdKey.set(movieId);
            // Emit movieId with count 1
            context.write(movieIdKey, oneValue);
        } catch (NumberFormatException e) {
            // Skip if parsing fails
        }
    }
}
