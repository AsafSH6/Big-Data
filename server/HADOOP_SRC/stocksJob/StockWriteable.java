package stocksJob;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import kmeans.KMeansable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import canopy.Canopyable;



public class StockWriteable extends CanopyAndKMeansCommon implements  WritableComparable<StockWriteable> {
	private double[] vector;
	private Text name;
	
	public static StockWriteable readStockFromCSV(String line) {
		String [] values = line.split(",");
		Text name= new Text(values[0]);
		double[] vector = new double[values.length - 1];
		for(int i=1; i < values.length ; i++) 
			vector[i - 1] = Double.parseDouble(values[i]);
		
		return new StockWriteable(vector, name);
	}
	
	public StockWriteable() {
		super();
		this.vector = new double[0];
		this.name = new Text();
		this.views = new IntWritable(1);
	}
	
	public StockWriteable(StockWriteable other) {
		super();
		double[] otherVector = other.vector;
		this.vector = new double[otherVector.length];
		for(int i=0; i < this.vector.length; i++)
			this.vector[i] = otherVector[i];
		this.name = new Text(other.name.toString());
		this.views = new IntWritable(other.getViews());
	}
	
	public StockWriteable(double [] vector, Text name) {
		super();
		this.vector = vector;
		this.name = name;
		this.views = new IntWritable(1);
	}
	
	public String getName() {
		return this.name.toString();
	}
	
	public void appendName(String name) {
		if(!this.name.toString().equals(""))
			this.name = new Text(this.name.toString() + ", " + name);
		else
			this.name = new Text(name);
	}
	
	public void mergeWithAnotherVector(double[] vector, String name) {
		for(int i=0; i < this.vector.length; i++) {
			this.vector[i] += vector[i];
		}
		appendName(name);
	}
	
	private double distance(StockWriteable other) {
		double[] otherVector = other.getVector();
		double distance = 0;
		for(int i=0; i < this.vector.length; i++) {
			double sub = this.vector[i] - otherVector[i];
			if(sub < 0)
				sub = -sub;
			distance += sub;
		}
		return distance; 
	}
	
	public String vectorAsString() {
		StringBuilder str = new StringBuilder();
		str.append('(');
		for(int i=0; i < this.vector.length; i++) {
			str.append(Double.toString(vector[i]));
			if(i < this.vector.length - 1)
				str.append(',');
		}	
		str.append(')');
		return str.toString();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.vector.length);
		for(int i=0; i < this.vector.length; i++)
			out.writeDouble(this.vector[i]);
		this.name.write(out);
		this.views.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.vector = new double[in.readInt()];
		for(int i=0; i < this.vector.length; i++)
			this.vector[i] = in.readDouble();
		this.name.readFields(in);
		this.views.readFields(in);
	}
	

	@Override
	public int compareTo(StockWriteable o) {
		double distance = this.distance(o);
		if(distance > 0)
			return 1;
		else if(distance < 0)
			return -1;
		else
			return 0;
	}
	
	@Override
	public String toString() {
		return this.name.toString();
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(vector);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		StockWriteable other = (StockWriteable) obj;
		if (!Arrays.equals(vector, other.vector))
			return false;
		return true;
	}
	
	

	@Override
	public double distance(Canopyable o) {
		return this.distance((StockWriteable)o);
	}

	@Override
	public double distance(KMeansable other) {
		return this.distance((StockWriteable)other);
	}
	
	public StockWriteable clone() {
		return new StockWriteable(this);
	}

	@Override
	public double[] getVector() {
		return this.vector;
	}

	@Override
	public void setVector(double[] vector) {
		this.vector = vector;
	}
	
}
