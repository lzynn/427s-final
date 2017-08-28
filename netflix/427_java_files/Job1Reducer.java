package stubs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
public class Job1Reducer extends Reducer<Text, Text, Text, Text> {

	Text out_key = new Text("");
	Text out_val = new Text("");

	/*
	 * The reduce method runs once for each key received from
	 * the shuffle and sort phase of the MapReduce framework.
	 * The method receives a key of type Text, a set of values of type
	 * IntWritable, and a Context object.
	 */
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		//		Map<Integer, String> list = new TreeMap<Integer, String>();
		List<String> list = new ArrayList<String>();

		for (Text value : values) {
			//			String[] user_stats = value.toString().split(",", 2);
			//			list.put(Integer.parseInt(user_stats[0]), user_stats[1]);

			list.add(value.toString());
		}

		String[] user1 = new String[2];
		String[] user2 = new String[2];
		int user1_id = 0;
		int user2_id = 0;

		for (int i=0; i<list.size()-1; ++i) {
			user1 = list.get(i).split(",", 2);
			user1_id = Integer.parseInt(user1[0]);
			for (int j=i+1; j<list.size(); ++j) {
				user2 = list.get(j).split(",", 2);
				user2_id = Integer.parseInt(user2[0]);
				if (user1_id < user2_id) {
					out_key.set(user1[0]+","+user2[0]);
					out_val.set(user1[1]+","+user2[1]);
					context.write(out_key, out_val);
				}
				else {
					out_key.set(user2[0]+","+user1[0]);
					out_val.set(user2[1]+","+user1[1]);
					context.write(out_key, out_val);
				}
			}
		}

	}
}
