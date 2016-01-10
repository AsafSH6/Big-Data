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
		canopy = new Canopy(0, 15);
	}
	
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	String[] numbers = value.toString().split(",");
//		System.out.println(Double.parseDouble(numbers[0]) + "," + Double.parseDouble(numbers[1]) + " " + numbers[0]);
		StockWriteable stock = StockWriteable.readStockFromCSV(value.toString());
		canopy.appendPoint(stock);
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	super.cleanup(context);
    	IntWritable one = new IntWritable(1);
    	for(Canopyable c: canopy) {
    		StockWriteable stock = (StockWriteable)c;
//    		System.out.println("center: " + stock + " views: " + stock.getViews());
    		context.write(one, stock);
    	}
    }
 }
