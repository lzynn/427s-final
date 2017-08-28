package stubs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* 
 * To define a reduce function for your MapReduce job, subclass 
 * the Reducer class and override the reduce method.
 * The class definition requires four parameters: 
 *   The data type of the input key (which is the output key type 
 *   from the mapper)
 *   The data type of the input value (which is the output value 
 *   type from the mapper)
 *   The data type of the output key
 *   The data type of the output value
 */   
public class prep_job_reducer extends Reducer<IntWritable, Text, Text, Text> {

	Text item = new Text("");
	Text num_sum_ratings = new Text("");

	/*
	 * The reduce method runs once for each key received from
	 * the shuffle and sort phase of the MapReduce framework.
	 * The method receives a key of type Text, a set of values of type
	 * IntWritable, and a Context object.
	 */
	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		int numRatings = 0;
		int sumRatings = 0;
		List<String> cache = new ArrayList<String>();

		/*
		 * For each value in the set of values passed to us by the mapper:
		 */
		for (Text value : values) {
			// value = rating,movie
			String[] rating_movie = value.toString().split(",");
			/*
			 * Add the value to the word count counter for this key.
			 */
			numRatings += 1;
			sumRatings += Integer.parseInt(rating_movie[0]);
			cache.add(value.toString());
		}

		for (String value : cache) {
			// rate_movie = [rating, movie-id]
			String[] rate_movie = value.split(",");
			// output key = movie-id
			// output value = (user,rating,numRatings,sumRatings)
			item.set(rate_movie[1]);
			num_sum_ratings.set(key.get()+","+rate_movie[0]+","+numRatings+","+sumRatings);
			context.write(item, num_sum_ratings);
		}

	}
}