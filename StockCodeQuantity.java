/**
 * The StockCodeQuantity class contains the main function that
 * runs a MapReduce job on Hadoop that will compute the quantity
 * of items sold, based on stock code. The dataset is based on
 * transactions for an online retailer, and was obtained from:
 * https://archive.ics.uci.edu/ml/datasets/Online+Retail
 *
 * Number of instances: 541909
 * Number of Attributes: 8
 *
 * @author  Abe Khaleghi
 * @version 1.0
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StockCodeQuantity {

	public static void main(String[] args) throws Exception {
    	if (args.length != 2) {
      		System.err.println("Usage: StockCodeQuantity <input path> <output path>");
      		System.exit(-1);
    	}

    	// Create a Hadoop job and set the main class
    	Job job = Job.getInstance();
    	job.setJarByClass(StockCodeQuantity.class);
    	job.setJobName("StockCodeQuantity");

    	// Set the input and output paths
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));

    	// Set the Mapper and Reducer classes
    	job.setMapperClass(StockCodeQuantityMapper.class);
    	job.setReducerClass(StockCodeQuantityReducer.class);

    	// Specify the type of the output for the key/value pairing
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(IntWritable.class);

    	// Run the job
    	System.exit(job.waitForCompletion(true) ? 0 : 1);
  	}
  	
}