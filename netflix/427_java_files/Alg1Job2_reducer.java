package stubs;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
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
public class Alg1Job2_reducer extends Reducer<Text, Text, Text, Text> {

	private Text out_val = new Text("");
	/*
	 * The reduce method runs once for each key received from
	 * the shuffle and sort phase of the MapReduce framework.
	 * The method receives a key of type Text, a set of values of type
	 * IntWritable, and a Context object.
	 */
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		double dotProd_norm = 0;
		double ratingAsqauredSum_norm = 0;
		double ratingUsquaredSum_norm = 0;

		for (Text value : values) {
			String[] stats = value.toString().split(",");
			if (stats.length == 6) {
				int a_rating = Integer.parseInt(stats[0]);
				double a_meanRatings = ((double)Integer.parseInt(stats[2])) / Integer.parseInt(stats[1]);

				int u_rating = Integer.parseInt(stats[3]);
				double u_meanRatings = ((double)Integer.parseInt(stats[5])) / Integer.parseInt(stats[4]);

				double a_norm = a_rating - a_meanRatings;
				double u_norm = u_rating - u_meanRatings;
				
				dotProd_norm += (a_norm * u_norm);
				ratingAsqauredSum_norm += (a_norm * a_norm);
				ratingUsquaredSum_norm += (u_norm * u_norm);
			}

		}

		double similarity = -1;
		if (ratingAsqauredSum_norm!=0 && ratingUsquaredSum_norm!=0) {
			similarity = dotProd_norm / (Math.sqrt(ratingAsqauredSum_norm) * Math.sqrt(ratingUsquaredSum_norm));
		}
		out_val.set(similarity+"");
		context.write(key, out_val);
		
	}
}
