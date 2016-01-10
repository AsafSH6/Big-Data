package stocksJob;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import kmeans.KMeansable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import canopy.Canopyable;



public class StockWriteable extends CanopyAndKMeansCommon implements  WritableComparable<StockWriteable> {
	private double[] vector;
	private Text name;
	
	public static StockWriteable readStockFromCSV(String line) {
		String [] values = line.split(";");
		Text name= new Text(values[0]);
		ArrayList<Double> days = new ArrayList<Double>();
		for (int i = 1; i < values.length; i++) {
			String [] stock= values[i].split(",");
			for (String string : stock) {
				days.add(new Double(Double.parseDouble(string)));
				
			}
		}
		double[] vector = new double[days.size()];
		for (int i = 0; i < vector.length; i++) {
			vector[i]= days.get(i);
			
		}
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
	
	private double distance(StockWriteable other) {
		double[] otherVector = other.getVector();
		double distance = 0;
		for(int i=0; i < this.vector.length; i++) {
//			double sub = (this.vector[i] - this.vector[i+1]) - (otherVector[i] - otherVector[i+1]);
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
		return this.name.toString() + " " + this.vectorAsString();		
	}
	
	@Override
	public boolean equals(Object o) {
		return this.distance((StockWriteable) o) == 0;
	}
	
	@Override
	public int hashCode() {
		return this.name.toString().hashCode();

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
