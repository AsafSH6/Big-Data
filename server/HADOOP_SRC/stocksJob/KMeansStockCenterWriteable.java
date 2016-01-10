package stocksJob;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import kmeans.KMeansCenter;

public class KMeansStockCenterWriteable extends KMeansCenter implements WritableComparable<KMeansStockCenterWriteable> {
	
	private StockWriteable canopyCenter;
	
	public KMeansStockCenterWriteable() {
		super(new StockWriteable());
		this.canopyCenter = new StockWriteable();
	}
	
	public KMeansStockCenterWriteable(KMeansStockCenterWriteable other) {
		super(other.kcenter.clone());
		this.canopyCenter = other.canopyCenter.clone();
	}

	public KMeansStockCenterWriteable(StockWriteable canopyCenter, StockWriteable center) {
		super(center);
		this.canopyCenter = canopyCenter;
	}
	
	public void setCanopyCenter(StockWriteable canopyCenter) {
		this.canopyCenter = canopyCenter;
	}
	
	public StockWriteable getCanopyCenter() {
		return this.canopyCenter;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		((StockWriteable)super.kcenter).write(out);
		this.canopyCenter.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		((StockWriteable)super.kcenter).readFields(in);
		this.canopyCenter.readFields(in);
	}

	@Override
	public int compareTo(KMeansStockCenterWriteable o) {
		return this.toString().compareTo(o.toString());
	}
	
	@Override
	public boolean equals(Object obj) {
		return this.toString().equals(obj.toString());
	}
	
	@Override
	public int hashCode() {
		return this.toString().hashCode();
	}
	
	@Override
	public KMeansStockCenterWriteable clone() {
		return new KMeansStockCenterWriteable(this);
	}
	
}
