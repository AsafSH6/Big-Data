package stocksJob;


import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class StocksMainDriver {
	
	final Path CANOPY_CENTERS_FILE = new Path("files/canopy-centers.seq");
	final Path CANOPY_CENTERS_AND_KMEANS_CENTERS_FILE = new Path("files/canopy-kmeans-centers.seq");
	String input;
	String output;
	int numberOfClusters;
	
	public StocksMainDriver(String input, String output, int numberOfClusters) {
		this.input = input;
		this.output = output;
		this.numberOfClusters = numberOfClusters;
	}
	
	public boolean canopyJob(float t1, float t2) throws IOException, InterruptedException, ClassNotFoundException {
	    Configuration conf = new Configuration();
	    
	    FileSystem fs = FileSystem.get(conf);
	    if(fs.exists(CANOPY_CENTERS_FILE))
	    	fs.delete(CANOPY_CENTERS_FILE, true);
	    
	    conf.setFloat("t1", t1);
	    conf.setFloat("t2", t2);

	    conf.set("canopy.centers.path", CANOPY_CENTERS_FILE.toString());

	    Job job = Job.getInstance(conf, "Canopy");
	    job.setJarByClass(StocksMainDriver.class);
	    job.setMapperClass(CanopyStocksMapper.class);
	    job.setReducerClass(CanopyStocksReducer.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(StockWriteable.class);
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    boolean status = job.waitForCompletion(true);
//	    System.out.println("canopy: " + job.getCounters().findCounter(CanopyStocksReducer.Counter.CANOPY_CENTERS).getValue());
//	    Thread.sleep(10000);
	    if (status)
	    	createSequenceFileOfCanopyCentersAndKMeansCentroids(conf, numberOfClusters);

	    return status;
	}
	
	public Job KMeansJob(int roundNumber, boolean lastRound) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(output), true);

	    conf.set("canopy.kmeans.center.path", CANOPY_CENTERS_AND_KMEANS_CENTERS_FILE.toString());
	    conf.setBoolean("kmeans.last.round", lastRound);

	    Job job = Job.getInstance(conf, "Kmeans " + roundNumber);
	    job.setJarByClass(StocksMainDriver.class);
	    job.setMapperClass(KMeansStockMapper.class);
	    job.setReducerClass(KMeansStockReducer.class);
	    job.setOutputKeyClass(KMeansStockCenterWriteable.class);
	    job.setOutputValueClass(StockWriteable.class);
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    job.waitForCompletion(true);
	    return job;
	}
	
	public boolean KMeansJobLoop() throws IOException, InterruptedException, ClassNotFoundException {
		int roundNumber = 1;
		boolean converged = false;
		while(!converged) {
			Job job = KMeansJob(roundNumber, false); 
		    converged = job.getCounters().findCounter(KMeansStockReducer.Counter.CONVERGED).getValue() == 0;
		    ++roundNumber;
		}
		
		KMeansJob(roundNumber, true);
		return true;
	}
	
	private void createSequenceFileOfCanopyCentersAndKMeansCentroids(Configuration conf, int NUMBER_OF_KCENTERS) throws IOException {
		final SequenceFile.Reader sqreader = new SequenceFile.Reader(conf,
				Reader.file(CANOPY_CENTERS_FILE));
		final SequenceFile.Writer seqWriter = SequenceFile.createWriter(conf,
                Writer.file(CANOPY_CENTERS_AND_KMEANS_CENTERS_FILE),
                Writer.keyClass(StockWriteable.class),
                Writer.valueClass(KMeansStockCenterWriteable.class));
		
		StockWriteable key = new StockWriteable(); // canopy center
		IntWritable value = new IntWritable(); // number of views
		sqreader.next(key, value);
		int totalViews = value.get();
		int centersAlreadyRandomized = 0;
		
		while(sqreader.next(key, value)) {
			HashSet<KMeansStockCenterWriteable> kcenters = createRandomStocksAsKMeansCenter(key, NUMBER_OF_KCENTERS, totalViews, value.get(), centersAlreadyRandomized);
			for(KMeansStockCenterWriteable center: kcenters) {
				seqWriter.append(key, center);
			}
			centersAlreadyRandomized += kcenters.size();
			totalViews -= value.get();
		}
		
		sqreader.close();
		seqWriter.close();
	}
	
	private HashSet<KMeansStockCenterWriteable> createRandomStocksAsKMeansCenter(StockWriteable canopyCenter, int NUMBER_OF_KCENTERS, int totalViews, int views, int centersAlreadyRandomized) {
		int lengthOfVector = canopyCenter.getVector().length;
		int numberOfKMeansCenters = NUMBER_OF_KCENTERS - centersAlreadyRandomized;
		numberOfKMeansCenters = (int)Math.ceil(((double)views / (double)totalViews) * numberOfKMeansCenters);
		if(numberOfKMeansCenters == 0)
			numberOfKMeansCenters = 1;
		
		HashSet<KMeansStockCenterWriteable> centers = new HashSet<KMeansStockCenterWriteable>();
		Random rand = new Random();
		
		for(int i=0; i < numberOfKMeansCenters; i++) {
			StockWriteable s = new StockWriteable();
			double[] vector = new double[lengthOfVector];
			for(int j=0; j < lengthOfVector; j++) {
				vector[j] = rand.nextDouble();
			}
			s.setVector(vector);
			centers.add(new KMeansStockCenterWriteable(canopyCenter.clone(), s));
		}
		System.out.println("randomized: " + centers.size() + " cetners");
		return centers;
	}

	public static void main(String[] args) throws Exception {
		FileUtils.deleteDirectory(new File(new Path(args[1]).toString()));
		
		StocksMainDriver mainDriver = new StocksMainDriver(args[0], args[1], Integer.parseInt(args[2]));
		
		mainDriver.canopyJob(0,48);

		mainDriver.KMeansJobLoop();
	}
	
}
