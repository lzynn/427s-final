package stubs;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *  Mapper's input are read from SequenceFile and records are: (K, V)
 *    where 
 *          K is a Text
 *          V is an Double
 *
 */
public class TopKMapper extends Mapper<Text, Text, IntWritable, Text> {

	IntWritable out_key = new IntWritable();
	Text out_val = new Text();

	@Override
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] users = key.toString().split(",");
		double similarity = Double.parseDouble(value.toString());

		if (users.length == 2) {
			String user_sim = users[1] + "," + similarity;
			out_key.set(Integer.parseInt(users[0]));
			out_val.set(user_sim);
			context.write(out_key,out_val);

			user_sim = users[0] + "," + similarity;
			out_key.set(Integer.parseInt(users[1]));
			out_val.set(user_sim);
			context.write(out_key,out_val);
		}

	}
}


