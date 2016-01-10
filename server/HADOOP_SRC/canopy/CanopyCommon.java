package canopy;

import org.apache.hadoop.io.IntWritable;

public abstract class CanopyCommon implements Canopyable{
	protected IntWritable views;
		
	@Override
	public void increaseViews(int views) {
		this.views = new IntWritable(this.views.get() + views);
	}

	@Override
	public int getViews() {
		return this.views.get();
	}
}
