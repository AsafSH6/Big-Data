package kmeans;

import java.util.HashSet;
import java.util.Iterator;


public class KMeans implements Iterable<KMeansCenter>{
	private HashSet<KMeansCenter> kcenters;
	
	public int size() {
		return kcenters.size();
	}
	
	public KMeans(HashSet<KMeansable> kcenters) {
		this.kcenters = new HashSet<KMeansCenter>();
		for(KMeansable center: kcenters) {
			this.kcenters.add(new KMeansCenter(center));
		}			
	}
	
	public KMeans() {
		this.kcenters = new HashSet<KMeansCenter>();
	}
	
	public void addCenter(KMeansCenter newCenter) {
		this.kcenters.add(newCenter.clone());
	}
	
	public KMeansCenter appendPoint(KMeansable point) {
		Iterator<KMeansCenter> centersIterator = kcenters.iterator();
		KMeansCenter cloestCenter = centersIterator.next();
		double minDistance = cloestCenter.distance(point);
		
		while(centersIterator.hasNext()) {
			KMeansCenter center = centersIterator.next();
			double distance = center.distance(point);
			if(distance < minDistance) {
				minDistance = distance;
				cloestCenter = center;
			}
		}
//		System.out.println(point + " is closest to " + cloestCenter);
		return cloestCenter;
	}

	@Override
	public Iterator<KMeansCenter> iterator() {
		return this.kcenters.iterator();
	}
	
}
