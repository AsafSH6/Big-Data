package stocksJob;

import java.io.IOException;
import java.util.HashMap;

import kmeans.KMeans;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import canopy.Canopyable;

public class KMeansStockMapper extends Mapper<Object, Text, KMeansStockCenterWriteable, StockWriteable> {
	
	private HashMap<StockWriteable, KMeans> canopyCentersAndTheirKMeans;
	private HashMap<KMeansStockCenterWriteable, StockWriteable> kmeansCenterToStocks;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		
		kmeansCenterToStocks = new HashMap<KMeansStockCenterWriteable, StockWriteable>();
		canopyCentersAndTheirKMeans = new HashMap<StockWriteable, KMeans>();

		SequenceFile.Reader sqreader = getSequenceFileReader(context);
		StockWriteable canopyCenter = new StockWriteable(); // key
		KMeansStockCenterWriteable kmeansCenter = new KMeansStockCenterWriteable(); // value
		while(sqreader.next(canopyCenter, kmeansCenter)) {
			KMeans kmeans = canopyCentersAndTheirKMeans.get(canopyCenter);
			if(kmeans == null) {
				kmeans = new KMeans();
				canopyCentersAndTheirKMeans.put(canopyCenter.clone(), kmeans);
			}
			kmeans.addCenter(kmeansCenter.clone());
		}
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StockWriteable stock = StockWriteable.readStockFromCSV(value.toString());
		if(stock == null)
			return;
		
		KMeans kmeans = findTheClosestCanopyCenter(stock);
		KMeansStockCenterWriteable center = (KMeansStockCenterWriteable) (kmeans.appendPoint(stock));

		StockWriteable collectionStock = kmeansCenterToStocks.get(center); // stock that collects all the stocks of this specific center
		if(collectionStock == null) {
			collectionStock = stock.clone();
			kmeansCenterToStocks.put(center, collectionStock);
		}
		else {
			collectionStock.mergeWithAnotherVector(stock.getVector(), stock.getName());
		}
		collectionStock.increaseViews(1);
    }
	
	private SequenceFile.Reader getSequenceFileReader(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		Path canopyCentersAndThierKMeansCentroids = new Path(conf.get("canopy.kmeans.center.path"));
		SequenceFile.Reader sqreader = new SequenceFile.Reader(conf, Reader.file(canopyCentersAndThierKMeansCentroids));
		return sqreader;
	}
	
	private KMeans findTheClosestCanopyCenter(StockWriteable stock) {
		KMeans kmeans = null;
		double minDistance = Double.MAX_VALUE;
		for(StockWriteable s: canopyCentersAndTheirKMeans.keySet()) {
			double distance = stock.distance((Canopyable)s);
			if(distance < minDistance) {
				minDistance = distance;
				kmeans = canopyCentersAndTheirKMeans.get(s);
			}
		}
		return kmeans;
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		for(KMeansStockCenterWriteable center: kmeansCenterToStocks.keySet()) {
			context.write(center, kmeansCenterToStocks.get(center));
		}
	}
	
}
