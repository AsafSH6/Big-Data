package stocksJob;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Reducer;

import canopy.Canopy;
import canopy.Canopyable;

public class CanopyStocksReducer extends Reducer<IntWritable, StockWriteable, IntWritable, StockWriteable> {
	
	public static enum Counter { CANOPY_CENTERS }
    private Canopy canopy;
    private int totalViews;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    	super.setup(context);
    	canopy = new Canopy(context.getConfiguration().getFloat("t1", 0), context.getConfiguration().getFloat("t2", 35));
    	this.totalViews = 0;
    }
    
    // Only one reducer will run since the key is always 1
    public void reduce(IntWritable key, Iterable<StockWriteable> values, Context context) throws IOException, InterruptedException {
    	for(StockWriteable s: values) 
    		canopy.appendPoint(s.clone()); 
    	
    	for(Canopyable c: canopy)
    		this.totalViews += c.getViews();
    	
    	this.writeCentersToSequenceFile(context);
    }
    
    private SequenceFile.Writer getSequenceFileWriter(Context context) throws IOException {
    	Configuration conf = context.getConfiguration();
    	Path canopyCenters = new Path(conf.get("canopy.centers.path"));
    	FileSystem fs = FileSystem.get(conf);
    	fs.delete(canopyCenters, true);
    	SequenceFile.Writer seqWriter = SequenceFile.createWriter(conf,
                Writer.file(canopyCenters), Writer.keyClass(StockWriteable.class),
                Writer.valueClass(IntWritable.class));
    	return seqWriter;
    }
    
    private void writeCentersToSequenceFile(Context context) throws IOException {
    	SequenceFile.Writer seqWriter = getSequenceFileWriter(context);
    	seqWriter.append(new StockWriteable(), new IntWritable(this.totalViews));
    	for(Canopyable c: canopy) {
    		context.getCounter(Counter.CANOPY_CENTERS).increment(1);
    		seqWriter.append((StockWriteable)c, new IntWritable(c.getViews()));
    	}
    	seqWriter.close();
    }
}
