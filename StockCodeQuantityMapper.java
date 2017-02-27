/**
 * The StockCodeQuantityMapper extends the predefined Mapper class
 * and overwrites the map function. The 4-tuple <LongWritable, Text,
 * Text, IntWritable> specifies that the input key-value pair is of
 * type <LongWritable, Text> and the output key-value type is of
 * type <Text, IntWritable>.
 *
 * @author  Abe Khaleghi
 * @version 1.0
 */

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StockCodeQuantityMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override public void map(LongWritable offset, Text lineText, Context context) 
  		throws IOException, InterruptedException {

		// Read a line
    	String line = lineText.toString();
    
    	// Get the stock code, which is the second item of the line
    	String stockCode = line.split(",")[1];
    
    	// Get the quantity sold, which is the fourth item of the line
    	int quantity = Integer.parseInt(line.split(",")[3]);
    
    	context.write(new Text(stockCode), new IntWritable(quantity));
	}
	
}
