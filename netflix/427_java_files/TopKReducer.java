package stubs;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TopKReducer extends Reducer<IntWritable, Text, Text, Text> {

	private int K = 10; // default

	private Text out_val = new Text("");
	private Text out_key = new Text("");

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		
		out_key.set(key.get()+"");
		
		SortedMap<Double, String> top = new TreeMap<Double, String>();
		for (Text value : values) {
			//value = user,similarity
			String[] tokens = value.toString().trim().split(",");
			String user = tokens[0];
			double similarity =  Double.parseDouble(tokens[1]);
			top.put(similarity, user);
			// keep only top K
			if (top.size() > K) {
				top.remove(top.firstKey());
			}
		}

		double sim = 0;
		String out_str = "";
		List<Double> keys = new ArrayList<Double>(top.keySet());
		for (int i=keys.size()-1; i>=0; i--) {
			sim = keys.get(i);
			out_str += (top.get(sim)+","+sim+",");
		}
		int str_len = out_str.length();
		if (out_str.substring(str_len-1, str_len).equals(",")) {
			out_str = out_str.substring(0, str_len-1);
		}
		out_val.set(out_str);
		context.write(out_key, out_val);
	}

	@Override
	protected void setup(Context context) 
			throws IOException, InterruptedException {
		this.K = context.getConfiguration().getInt("K", 10); // default is top 5

	}


}