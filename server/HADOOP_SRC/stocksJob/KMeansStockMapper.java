package stocksJob;

import java.io.IOException;
import java.util.HashMap;

import kmeans.KMeans;
import kmeans.KMeansCenter;
import kmeans.KMeansable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import canopy.Canopyable;

public class KMeansStockMapper extends Mapper<Object, Text, KMeansStockCenterWriteable, StockWriteable> {
	
	private HashMap<StockWriteable, KMeans> canopyCentersAndTheirKMeans;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		System.out.println("MAP");
		
		canopyCentersAndTheirKMeans = new HashMap<StockWriteable, KMeans>();
		Configuration conf = context.getConfiguration();
		Path canopyCentersAndThierKMeansCentroids = new Path(conf.get("canopy.kmeans.center.path"));
		FileSystem fs =  FileSystem.get(conf);
		SequenceFile.Reader sqreader = new SequenceFile.Reader(fs, canopyCentersAndThierKMeansCentroids, conf);
		StockWriteable canopyCenter = new StockWriteable();
		KMeansStockCenterWriteable kmeansCenter = new KMeansStockCenterWriteable();
		while(sqreader.next(canopyCenter, kmeansCenter)) {
//			System.out.println("canopy: " + canopyCenter + " kmean center" + kmeansCenter);
			KMeans kmeans = canopyCentersAndTheirKMeans.get(canopyCenter);
			if(kmeans == null) {
				kmeans = new KMeans();
				canopyCentersAndTheirKMeans.put(canopyCenter.clone(), kmeans);
			}
			kmeans.addCenter(kmeansCenter.clone());
		}
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	String[] numbers = value.toString().split(",");
//		System.out.println(Double.parseDouble(numbers[0]) + "," + Double.parseDouble(numbers[1]) + " " + numbers[2]);
		StockWriteable stock = StockWriteable.readStockFromCSV(value.toString());
		
		KMeans kmeans = findTheClosestCanopyCenter(stock);
		KMeansStockCenterWriteable center = (KMeansStockCenterWriteable) (kmeans.appendPoint(stock));
//		KMeansable center = kmeans.appendPoint(stock).getCenter();
		context.write(center, stock);
    }
	
	private KMeans findTheClosestCanopyCenter(StockWriteable stock) {
		KMeans kmeans = null;
		double minDistance = Double.MAX_VALUE;
		for(StockWriteable s: canopyCentersAndTheirKMeans.keySet()) {
			double distance = stock.distance((Canopyable)s);
//			System.out.println("distance between: " + stock + " and: " + s + " is: " + distance);
			if(distance < minDistance) {
//				System.out.println(stock + " is now closest to: " + s);
				minDistance = distance;
				kmeans = canopyCentersAndTheirKMeans.get(s);
			}
		}
		return kmeans;
	}
	
}
