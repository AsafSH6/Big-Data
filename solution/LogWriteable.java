package solution;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class LogWriteable implements WritableComparable<LogWriteable> {
	
	private Text hostname;
	private Text username;
	private Text date;
	private Text hour;
	
	public LogWriteable() {
		this.hostname = new Text();
		this.username = new Text();
		this.date = new Text();
		this.hour = new Text();
	}
	
	public LogWriteable(String hostname, String date, String hour, String username) {
		this.hostname = new Text(hostname);
		this.username = new Text(username);
		this.date = new Text(date);
		this.hour = new Text(hour);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.hostname.write(out);
		this.username.write(out);
		this.date.write(out);
		this.hour.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.hostname.readFields(in);
		this.username.readFields(in);
		this.date.readFields(in);
		this.hour.readFields(in);
	}
	

	@Override
	public int compareTo(LogWriteable o) {
		return this.toString().compareTo(o.toString());

	}
	
	@Override
	public String toString() {
	    return this.username.toString()+ " " + this.hostname.toString() + " " + this.date.toString();

	}
	
	@Override
	public boolean equals(Object o) {
		if(o instanceof LogWriteable) {
			return this.hostname.equals(((LogWriteable) o).hostname) && this.username.equals(((LogWriteable) o).username) && this.date.equals(((LogWriteable) o).date);

		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return this.toString().hashCode();
	}


}
