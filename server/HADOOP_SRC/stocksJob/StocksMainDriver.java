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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


// to change the static methods....
// create stock main driver singleton
public class StocksMainDriver {
	
	final static Path CANOPY_CENTERS_FILE = new Path("files/canopy-centers.seq");
	final static Path CANOPY_CENTERS_AND_KMEANS_CENTERS_FILE = new Path("files/canopy-kmeans-centers.seq");
	
	public static boolean canopyJob(String input, String output, int numberOfClusters) throws IOException, InterruptedException, ClassNotFoundException {
	    Configuration conf = new Configuration();
	    
	    // Delete old files
	    FileSystem fs = FileSystem.get(conf);
	    if(fs.exists(CANOPY_CENTERS_FILE))
	    	fs.delete(CANOPY_CENTERS_FILE, true);
	    
	    // Set canopy centers sequence file path
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
	    if (status)
	    	createSequenceFileOfCanopyCentersAndKMeansCentroids(conf, numberOfClusters);
	    return status;
	}
	
	public static Job KMeansJob(String[] args, boolean lastRound) throws IOException, InterruptedException, ClassNotFoundException {
		final int NUMBER_OF_KCENTERS = Integer.parseInt(args[2]);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]), true);

	    // Set canopy centers sequence file path
	    conf.set("canopy.kmeans.center.path", CANOPY_CENTERS_AND_KMEANS_CENTERS_FILE.toString());
	    conf.setBoolean("kmeans.last.round", lastRound);

	    Job job = Job.getInstance(conf, "Kmeans");
	    job.setJarByClass(StocksMainDriver.class);
	    job.setMapperClass(KMeansStockMapper.class);
	    job.setReducerClass(KMeansStockReducer.class);
	    job.setOutputKeyClass(KMeansStockCenterWriteable.class);
	    job.setOutputValueClass(StockWriteable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    job.waitForCompletion(true);
	    return job;
	}
	
	public static boolean KMeansJobLoop(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		int rounds = 0;
		boolean converged = false;
		while(!converged) {
			++rounds;

			Job job = KMeansJob(args, false);   
		    converged = job.getCounters().findCounter(KMeansStockReducer.Counter.CONVERGED).getValue() == 0;
//		    System.out.println("converged: " + converged);
//		    System.out.println("round: " + rounds);
//		    Thread.sleep(10000);
		}
		KMeansJob(args, true);
		return true;
	}
	
	private static void createSequenceFileOfCanopyCentersAndKMeansCentroids(Configuration conf, int NUMBER_OF_KCENTERS) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		StockWriteable key = new StockWriteable();
		IntWritable value = new IntWritable();
		final SequenceFile.Reader sqreader = new SequenceFile.Reader(fs, CANOPY_CENTERS_FILE, conf);
		final SequenceFile.Writer sqwriter = SequenceFile.createWriter(fs, conf, CANOPY_CENTERS_AND_KMEANS_CENTERS_FILE, StockWriteable.class, KMeansStockCenterWriteable.class);
		sqreader.next(key, value);
		int totalViews = value.get();
		int centersAlreadyRandomized = 0;
		while(sqreader.next(key, value)) {
			HashSet<KMeansStockCenterWriteable> kcenters = createRandomStocksAsKMeansCenter(key, NUMBER_OF_KCENTERS, totalViews, value.get(), centersAlreadyRandomized);
			for(KMeansStockCenterWriteable center: kcenters) {
				sqwriter.append(key, center);
			}
			centersAlreadyRandomized += kcenters.size();
			totalViews -= value.get();
		}
		sqreader.close();
		sqwriter.close();
	}
	
	private static HashSet<KMeansStockCenterWriteable> createRandomStocksAsKMeansCenter(StockWriteable canopyCenter, int NUMBER_OF_KCENTERS, int totalViews, int views, int centersAlreadyRandomized) {
		int lengthOfVector = canopyCenter.getVector().length;
		int numberOfKMeansCenters = NUMBER_OF_KCENTERS - centersAlreadyRandomized;
		numberOfKMeansCenters = (int)Math.ceil(((double)views / (double)totalViews) * numberOfKMeansCenters);
//		System.out.println("getting " + numberOfKMeansCenters + "kcenters");
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
		
//		// TEST
//		System.out.println("TEST MODE");
//		if(views == 4) {
//			StockWriteable s1 = new StockWriteable(0.7, 0.8, "1");
//			centers.add(new KMeansStockCenterWriteable(canopyCenter.clone(), s1));
//		}
//		else {
//			StockWriteable s2 = new StockWriteable(0.3, 0.2, "2");
//			StockWriteable s3 = new StockWriteable(0, 0.4, "3");
//			centers.add(new KMeansStockCenterWriteable(canopyCenter.clone(), s2));
//			centers.add(new KMeansStockCenterWriteable(canopyCenter.clone(), s3));
//		}
		return centers;
	}

	public static void main(String[] args) throws Exception {
		FileUtils.deleteDirectory(new File(new Path(args[1]).toString()));
//	    Configuration conf = new Configuration();
//	    
//	    // Delete old files
//	    FileSystem fs = FileSystem.get(conf);
//	    if(fs.exists(CANOPY_CENTERS_FILE))
//	    	fs.delete(CANOPY_CENTERS_FILE, true);
//	    
//	    // Set canopy centers sequence file path
//	    conf.set("canopy.centers.path", CANOPY_CENTERS_FILE.toString());
//
//	    Job job = Job.getInstance(conf, "Canopy");
//	    job.setJarByClass(StocksMainDriver.class);
//	    job.setMapperClass(CanopyStocksMapper.class);
//	    job.setReducerClass(CanopyStocksReducer.class);
//	    job.setOutputKeyClass(IntWritable.class);
//	    job.setOutputValueClass(StockWriteable.class);
//	    FileInputFormat.addInputPath(job, new Path(args[0]));
//	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
//	    boolean status = job.waitForCompletion(true);
//	    if (status)
//	    	createSequenceFileOfCanopyCentersAndKMeansCentroids(conf, 2);
//	    
//	    int rounds = 0;
//		boolean converged = false;
//		while(!converged) {
//			++rounds;
//			FileUtils.deleteDirectory(new File(new Path(args[1]).toString()));
//
//			conf = new Configuration();
//			
//		    // Set canopy centers sequence file path
//		    conf.set("canopy.kmeans.center.path", CANOPY_CENTERS_AND_KMEANS_CENTERS_FILE.toString());
//	
//		    job = Job.getInstance(conf, "Kmeans");
//		    job.setJarByClass(StocksMainDriver.class);
//		    job.setMapperClass(KMeansStockMapper.class);
//		    job.setReducerClass(KMeansStockReducer.class);
//		    job.setOutputKeyClass(KMeansStockCenterWriteable.class);
//		    job.setOutputValueClass(StockWriteable.class);
//		    FileInputFormat.addInputPath(job, new Path(args[0]));
//		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		    job.waitForCompletion(true);
//		    
//		    converged = job.getCounters().findCounter(KMeansStockReducer.Counter.CONVERGED).getValue() == 0;
//		    System.out.println("converged: " + converged);
//		    System.out.println("round: " + rounds);
//	}
		StocksMainDriver.canopyJob(args[0], args[1], Integer.parseInt(args[2]));
		StocksMainDriver.KMeansJobLoop(args);
	}
	
}
