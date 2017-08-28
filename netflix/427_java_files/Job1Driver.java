package stubs;

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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;



public class Job1Driver  extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());

		job.setJarByClass(Job1Driver.class);
		job.setJobName("Alg3Job1Driver");
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Mapper.class);
		job.setReducerClass(Job1Reducer.class);

		job.setNumReduceTasks(8);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);


		boolean status = job.waitForCompletion(true);


		return status ? 0 : 1;
	}

	/**
	 * The main driver for "Aggregate By Key" program.
	 * Invoke this method to submit the map/reduce job.
	 * @throws Exception When there is communication problems with the job tracker.
	 */
	public static void main(String[] args) throws Exception {
		
		// Make sure there are exactly 2 parameters
		if (args.length != 2) {
			System.exit(1);
		}

		int returnStatus = ToolRunner.run(new Configuration(), new Job1Driver(), args);
		System.exit(returnStatus);
	}

}