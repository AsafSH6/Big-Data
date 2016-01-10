package stocksJob;

import kmeans.KMeansable;

import org.apache.hadoop.io.IntWritable;

import canopy.Canopyable;

public abstract class CanopyAndKMeansCommon implements Canopyable, KMeansable{

	protected IntWritable views;


	@Override
	public void increaseViews(int views) {
		this.views.set(this.views.get() + views);
	}

	@Override
	public int getViews() {
		return this.views.get();
	}
	
	public abstract KMeansable clone();

}
