/**
 * The StockCodeQuantityReducer extends the predefined Reducer
 * class and overwrites the reduce function. The 4-tuple <Text,
 * IntWritable, Text, IntWritable > specifies the types of the
 * input and output key-value pair. The type of the input
 * key-value pair (<Text, IntWritable>) is the same as the output
 * key-value pair of the mapper.
 *
 * @author  Abe Khaleghi
 * @version 1.0
 */

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StockCodeQuantityReducer extends Reducer<Text ,  IntWritable ,  Text ,  IntWritable > {

	@Override public void reduce(Text stockCode, Iterable<IntWritable> counts, Context context)
		throws IOException,  InterruptedException {

		// Count totals
      	int sum  = 0;
      	for ( IntWritable count  : counts) {
        	sum  += count.get();
      	}
      	
      	context.write(stockCode, new IntWritable(sum));
    }
}