package stocksJob;

import java.io.IOException;
import java.util.HashSet;

import kmeans.KMeansable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansStockReducer extends Reducer<KMeansStockCenterWriteable, KMeansable, KMeansStockCenterWriteable, KMeansable>{
	
	public static enum Counter { CONVERGED }
	private SequenceFile.Writer seqWriter;
	private boolean lastRound;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

		this.seqWriter = this.getSequenceFileWriter(context);
		this.lastRound = context.getConfiguration().getBoolean("kmeans.last.round", false);
	}
	
    public void reduce(KMeansStockCenterWriteable key, Iterable<KMeansable> values, Context context) throws IOException, InterruptedException {

    	HashSet<KMeansable> points = deepCopyOfIterableIntoHashSet(values);
    	boolean hasChanged = key.calculateNewCenter(points.iterator());
    	if(hasChanged)
    		context.getCounter(Counter.CONVERGED).increment(1);
    	
    	if(this.lastRound == true){
        	StockWriteable stock = new StockWriteable();
	    	for(KMeansable k: points) {
	    		stock.appendName(((StockWriteable)k).getName());
	    	}
	    	context.write(key, stock);
    	}
    	else {
    		this.seqWriter.append(key.getCanopyCenter(), key);
    	}
    }
    
    private HashSet<KMeansable> deepCopyOfIterableIntoHashSet(Iterable<KMeansable> iter) {
    	HashSet<KMeansable> newIter = new HashSet<KMeansable>();
    	for(KMeansable k: iter) 
    		newIter.add(k.clone());
    	
    	return newIter;
    }
    
    private SequenceFile.Writer getSequenceFileWriter(Context context) throws IOException {
    	Configuration conf = context.getConfiguration();
    	Path canopyCentersAndThierKMeansCentroids = new Path(conf.get("canopy.kmeans.center.path"));
    	FileSystem fs = FileSystem.get(conf);
    	fs.delete(canopyCentersAndThierKMeansCentroids, true);
    	SequenceFile.Writer seqWriter = SequenceFile.createWriter(conf,
                Writer.file(canopyCentersAndThierKMeansCentroids), Writer.keyClass(StockWriteable.class),
                Writer.valueClass(KMeansStockCenterWriteable.class));
    	return seqWriter;
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	super.cleanup(context);
    	
    	this.seqWriter.close();
    }

}
