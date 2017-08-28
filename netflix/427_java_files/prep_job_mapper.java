package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* 
 * To define a map function for your MapReduce job, subclass 
 * the Mapper class and override the map method.
 * The class definition requires four parameters: 
 *   The data type of the input key
 *   The data type of the input value
 *   The data type of the output key (which is the input key type 
 *   for the reducer)
 *   The data type of the output value (which is the input value 
 *   type for the reducer)
 */

public class prep_job_mapper extends Mapper<LongWritable, Text, IntWritable, Text> {

  /*
   * The map method runs once for each line of text in the input file.
   * The method receives a key of type LongWritable, a value of type
   * Text, and a Context object.
   */
	Text rating_movie = new Text("");
	IntWritable user_id = new IntWritable(0);
	
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    /*
     * Convert the line, which is received as a Text object,
     * to a String object.
     */
    String line = value.toString().trim();

    /*
     * The line.split("\\W+") call uses regular expressions to split the
     * line up by non-word characters.
     * 
     * If you are not familiar with the use of regular expressions in
     * Java code, search the web for "Java Regex Tutorial." 
     */
    String[] words = line.split(",");
      if (words.length == 3) {
        
        /*
         * Call the write method on the Context object to emit a key
         * and a value from the map method.
         */
    	  String movie_id = words[0];
    	  user_id.set(Integer.parseInt(words[1]));
    	  rating_movie.set((int)Double.parseDouble(words[2]) + "," + movie_id);
        context.write(user_id, rating_movie);
      
    }
  }
}