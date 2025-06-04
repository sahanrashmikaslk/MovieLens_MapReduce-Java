package movielens;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper for calculating average rating per movie.
 * Emits (movieId, rating) as (IntWritable, DoubleWritable).
 */
public class AverageRatingMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

    private IntWritable movieIdKey = new IntWritable();
    private DoubleWritable ratingValue = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Each line of input format: userId,movieId,rating,timestamp
        String line = value.toString().trim();
        if (line.isEmpty()) {
            return; // skip empty lines (if any)
        }
        // Skip header line if present
        if (line.startsWith("userId")) {
            return;
        }

        String[] fields = line.split(",");  // CSV separated by commas
        if (fields.length < 3) {
            // Malformed line (not enough fields), skip it
            return;
        }
        try {
            int movieId = Integer.parseInt(fields[1].trim());
            double rating = Double.parseDouble(fields[2].trim());
            movieIdKey.set(movieId);
            ratingValue.set(rating);
            // Emit movieId as key and rating as value
            context.write(movieIdKey, ratingValue);
        } catch (NumberFormatException e) {
            // Skip lines where parsing fails (e.g., bad data)
        }
    }
}
