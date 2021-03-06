package kmeans;

import java.util.Iterator;


public class KMeansCenter implements KMeansable{
	protected KMeansable kcenter;
	
	public KMeansCenter(KMeansCenter other) {
		this.kcenter = other.kcenter.clone();
	}
	
	public KMeansCenter(KMeansable center) {
		this.kcenter = center.clone();
	}
	
	public KMeansable getCenter() {
		return this.kcenter;
	}

	@Override
	public double distance(KMeansable other) {
		return kcenter.distance(other);
	}
	
	@Override
	public KMeansCenter clone() {
		return new KMeansCenter(this);
	}

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append('(');
		double[] vector = this.kcenter.getVector();
		for(int i=0; i < vector.length; i++) {
			str.append(Double.toString(vector[i]));
			if(i < vector.length - 1)
				str.append(',');
		}	
		str.append(')');
		return str.toString();
	}

	@Override
	public double[] getVector() {
		return this.kcenter.getVector();
	}

	@Override
	public void setVector(double[] vector) {
		this.kcenter.setVector(vector);
	}
	
	public boolean calculateNewCenter(Iterator<KMeansable> points) {
		double[] newCenter = new double[this.kcenter.getVector().length];
		int totalViews = 0;
		while(points.hasNext()) {
			KMeansable point = points.next();
			
			double[] pointVector = point.getVector();
			totalViews += point.getViews();
			
			for(int i=0; i < newCenter.length; i++) {
				newCenter[i] += pointVector[i];
			}
		}
		for(int i=0; i < newCenter.length; i++) {
			 newCenter[i] /= totalViews; 
		}
		boolean hadChanged = !areVectorsEquals(newCenter);
		if(hadChanged)
			this.kcenter.setVector(newCenter);
		
		return hadChanged;
	}
	
	private boolean areVectorsEquals(double[] other) {
		double[] vector = this.kcenter.getVector();
		for(int i=0; i < vector.length; i++) {
			if(vector[i] != other[i])
				return false;
		}
		return true;
	}

	@Override
	public void increaseViews(int views) {
		this.kcenter.increaseViews(views);
	}

	@Override
	public int getViews() {
		return this.kcenter.getViews();
	}
}
