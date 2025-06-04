package movielens;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for calculating average rating per movie.
 * Receives (movieId, [rating1, rating2, ...]) and outputs (movieId, avgRating).
 */
public class AverageRatingReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

    private DoubleWritable result = new DoubleWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double sum = 0.0;
        long count = 0;
        // Sum all ratings and count them
        for (DoubleWritable val : values) {
            sum += val.get();
            count++;
        }
        if (count == 0) {
            return;
        }
        double average = sum / count;
        result.set(average);
        // Write out the movieId and its average rating
        context.write(key, result);
    }
}
