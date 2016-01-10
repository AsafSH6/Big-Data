package canopy;


public interface Canopyable {
	public double distance(Canopyable o);
	public void increaseViews(int views);
	public int getViews();
}
