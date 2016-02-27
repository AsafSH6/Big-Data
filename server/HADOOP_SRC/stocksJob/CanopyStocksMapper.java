package stocksJob;

import java.io.IOException;

import kmeans.KMeansable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import canopy.Canopy;
import canopy.Canopyable;

public class CanopyStocksMapper extends Mapper<Object, Text, IntWritable, KMeansable>{
	private Canopy canopy;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
    	canopy = new Canopy(context.getConfiguration().getFloat("t1", 0), context.getConfiguration().getFloat("t2", 35));
	}
	
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StockWriteable stock = StockWriteable.readStockFromCSV(value.toString());
		if(stock == null)
			return;
		canopy.appendPoint(stock);
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	super.cleanup(context);
    	IntWritable one = new IntWritable(1);
    	for(Canopyable c: canopy) {
    		context.write(one, (StockWriteable)c);
    	}
    }
 }
