package stocksJob;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;

import canopy.Canopy;
import canopy.Canopyable;

public class CanopyStocksReducer extends Reducer<IntWritable, StockWriteable, IntWritable, StockWriteable> {
    private Canopy canopy;
    private int totalViews;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    	super.setup(context);
//    	System.out.println("REDUCER");
    	canopy = new Canopy(0, 18);
    	this.totalViews = 0;
    }
    
    // Only one reducer will run since the key is always 1
    public void reduce(IntWritable key, Iterable<StockWriteable> values, Context context) throws IOException, InterruptedException {
    	for(StockWriteable s: values) {
    		canopy.appendPoint(s.clone());
    	}
    	
    	for(Canopyable c: canopy) {
    		this.totalViews += c.getViews();
//    		context.write(new IntWritable(c.getViews()), (StockWriteable)c);
    	}
    	this.writeCentersToSequenceFile(context);
    }
    
    private void writeCentersToSequenceFile(Context context) throws IOException {
    	Configuration conf = context.getConfiguration();
    	Path outputPath = new Path(conf.get("canopy.centers.path"));
    	FileSystem fs = FileSystem.get(conf);
    	fs.delete(outputPath, true);
    	final SequenceFile.Writer sfout = SequenceFile.createWriter(fs, conf, outputPath, StockWriteable.class, IntWritable.class);
    	sfout.append(new StockWriteable(), new IntWritable(this.totalViews)); // Write the total stocks
//    	System.out.println("number of centers: " + canopy.getCentroids().size());
    	for(Canopyable c: canopy) {
    		sfout.append((StockWriteable)c, new IntWritable(c.getViews()));
    	}
    	sfout.close();
    }
}
