package stocksJob;

import java.io.IOException;
import java.util.HashSet;

import kmeans.KMeansable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansStockReducer  extends Reducer<KMeansStockCenterWriteable, KMeansable, KMeansStockCenterWriteable, KMeansable>{
	
	public static enum Counter { CONVERGED }
	HashSet<KMeansStockCenterWriteable> allKMeanCenters;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		this.allKMeanCenters = new HashSet<KMeansStockCenterWriteable>();
	}
	
    public void reduce(KMeansStockCenterWriteable key, Iterable<KMeansable> values, Context context) throws IOException, InterruptedException {
//    	System.out.println("KMEANS REDUCER");

    	HashSet<KMeansable> points = deepCopyOfIterableIntoHashSet(values);
    	boolean converged = key.calculateNewCenter(points.iterator());
    	if(converged)
    		context.getCounter(Counter.CONVERGED).increment(1);
    	
    	allKMeanCenters.add(key.clone());
    	if(context.getConfiguration().getBoolean("kmeans.last.round", false)){
	    	for(KMeansable k: points) {
	    		context.write(key, k);
	    	}
    	}
    }
    
    private HashSet<KMeansable> deepCopyOfIterableIntoHashSet(Iterable<KMeansable> iter) {
    	HashSet<KMeansable> newIter = new HashSet<KMeansable>();
    	for(KMeansable k: iter) {
    		newIter.add(k.clone());
    	}
    	return newIter;
    }
    
    private SequenceFile.Writer getSequenceFileWriter(Context context) throws IOException {
    	Configuration conf = context.getConfiguration();
    	Path canopyCentersAndThierKMeansCentroids = new Path(conf.get("canopy.kmeans.center.path"));
    	FileSystem fs = FileSystem.get(conf);
    	fs.delete(canopyCentersAndThierKMeansCentroids, true);
    	SequenceFile.Writer seqWriter = SequenceFile.createWriter(fs, conf, canopyCentersAndThierKMeansCentroids, StockWriteable.class, KMeansStockCenterWriteable.class);
    	return seqWriter;
    }
    // move to main
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	super.cleanup(context);
    	if(!context.getConfiguration().getBoolean("kmeans.last.round", false)) {
	    	SequenceFile.Writer seqWriter = getSequenceFileWriter(context);
	    	for(KMeansStockCenterWriteable center: allKMeanCenters) {
	        	seqWriter.append(center.getCanopyCenter(), center);
	//    		System.out.println("wrote the kmeans center: " + center +" that belongs to canopy center: " + center.getCanopyCenter());
	    	}
	//    	System.out.println("wrote " + allKMeanCenters.size() + "kmeans centers");
	    	seqWriter.close();
    	}
    }

}
