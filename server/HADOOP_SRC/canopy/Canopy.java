package canopy;
import java.util.HashSet;
import java.util.Iterator;



public class Canopy implements Iterable<Canopyable>{
	final private double t1;
	final private double t2;
	private HashSet<Canopyable> centroids;
	
	public Canopy(double t2, double t1) {
		this.t1 = t1;
		this.t2 = t2;
		this.centroids = new HashSet<Canopyable>();
	}
	
	public void appendPoint(Canopyable point) {
		boolean match = false;
		for(Canopyable c : centroids) {
			double distance = c.distance(point);
			if(distance < t1 && distance > t2) {
//				System.out.println(point + " merged into: " + c);
				c.increaseViews(point.getViews());
				match = true;
			}
		}
		if(!match) {
			centroids.add(point);
		}
	}
	
	public HashSet<Canopyable> getCentroids() {
		return this.centroids;
	}

	@Override
	public Iterator<Canopyable> iterator() {
		return this.centroids.iterator();
	}
}
