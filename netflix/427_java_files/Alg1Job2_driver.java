package stubs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
/* 
 * MapReduce jobs are typically implemented by using a driver class.
 * The purpose of a driver class is to set up the configuration for the
 * MapReduce job and to run the job.
 * Typical requirements for a driver class include configuring the input
 * and output data formats, configuring the map and reduce classes,
 * and specifying intermediate data formats.
 * 
 * The following is the code for the driver class:
 */
public class Alg1Job2_driver extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
	  int exitCode = ToolRunner.run(new Configuration(), new Alg1Job2_driver(), args);
	  System.exit(exitCode);
    
  }
  
  public int run(String[] args) throws Exception {
	  /*
	     * The expected command-line arguments are the paths containing
	     * input and output data. Terminate the job if the number of
	     * command-line arguments is not exactly 2.
	     */
	    if (args.length != 2) {
	      System.out.printf(
	          "Usage: %s [generic options] <input dir> <output dir> \n", getClass().getSimpleName());
	      return -1;
	    }

	    /*
	     * Instantiate a Job object for your job's configuration.  
	     */
	    Job job = new Job(getConf());
	    
	    /*
	     * Specify the jar file that contains your driver, mapper, and reducer.
	     * Hadoop will transfer this jar file to nodes in your cluster running
	     * mapper and reducer tasks.
	     */
	    job.setJarByClass(Alg1Job2_driver.class);
	    
	    /*
	     * Specify an easily-decipherable name for the job.
	     * This job name will appear in reports and logs.
	     */
	    job.setJobName("Algorithm 1 Job 2");

	    /*
	     * Specify the paths to the input and output data based on the
	     * command-line arguments.
	     */
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setInputFormatClass(KeyValueTextInputFormat.class);
	    
//	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
//		FileOutputFormat.setCompressOutput(job, true);
//		FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
//		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);

	    
	    /*
	     * Specify the mapper and reducer classes.
	     */
	    job.setMapperClass(Mapper.class);
	    job.setReducerClass(Alg1Job2_reducer.class);
	    /*
	     * For the word count application, the input file and output 
	     * files are in text format - the default format.
	     * 
	     * In text format files, each record is a line delineated by a 
	     * by a line terminator.
	     * 
	     * When you use other input formats, you must call the 
	     * SetInputFormatClass method. When you use other 
	     * output formats, you must call the setOutputFormatClass method.
	     */
	      
	    /*
	     * For the word count application, the mapper's output keys and
	     * values have the same data types as the reducer's output keys 
	     * and values: Text and IntWritable.
	     * 
	     * When they are not the same data types, you must call the 
	     * setMapOutputKeyClass and setMapOutputValueClass 
	     * methods.
	     */
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    /*
	     * Specify the job's output key and value classes.
	     */
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    /*
	     * Start the MapReduce job and wait for it to finish.
	     * If it finishes successfully, return 0. If not, return 1.
	     */
	    boolean success = job.waitForCompletion(true);
	    return success ? 0 : 1;
  }
}
