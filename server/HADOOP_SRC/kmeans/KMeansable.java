package kmeans;


public interface KMeansable {
	public double distance(KMeansable other);
	public KMeansable clone();
	public double[] getVector();
	public void setVector(double[] vector);
}
