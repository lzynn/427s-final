package stubs;

import org.apache.log4j.Logger;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class TopKDriver  extends Configured implements Tool {

	private static Logger THE_LOGGER = Logger.getLogger(TopKDriver.class);

	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		int K = Integer.parseInt(args[0]); // top K
		job.getConfiguration().setInt("K", K);
		job.setJobName("TopNDriver");
		job.setJarByClass(TopKDriver.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
//		job.setOutputFormatClass(SequenceFileOutputFormat.class);
//		FileOutputFormat.setCompressOutput(job, true);
//		FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
//		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);

		job.setMapperClass(TopKMapper.class);
		job.setReducerClass(TopKReducer.class);

		// map()'s output (K,V)
		job.setMapOutputKeyClass(IntWritable.class);   
		job.setMapOutputValueClass(Text.class);   

		// reduce()'s output (K,V)
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// args[1] = input directory
		// args[2] = output directory
		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		boolean status = job.waitForCompletion(true);
		THE_LOGGER.info("run(): status="+status);
		return status ? 0 : 1;
	}

	/**
	 * The main driver for "Top K" program.
	 * Invoke this method to submit the map/reduce job.
	 * @throws Exception When there is communication problems with the job tracker.
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			THE_LOGGER.warn("usage TopNDriver <N> <input> <output>");
			System.exit(1);
		}

		int returnStatus = ToolRunner.run(new Configuration(), new TopKDriver(), args);
		System.exit(returnStatus);
	}

}