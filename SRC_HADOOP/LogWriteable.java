package solution;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class LogWriteable implements WritableComparable<LogWriteable> {
	
	private Text hostname;
	private Text username;
	
	public LogWriteable() {
		this.hostname = new Text();
		this.username = new Text();
	}
	
	public LogWriteable(String hostname, String username) {
		this.hostname = new Text(hostname);
		this.username = new Text(username);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.hostname.write(out);
		this.username.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.hostname.readFields(in);
		this.username.readFields(in);
	}
	

	@Override
	public int compareTo(LogWriteable o) {
		return this.toString().compareTo(o.toString());
	}
	
	@Override
	public String toString() {
	    return this.username.toString()+ " " + this.hostname.toString();
	}
	
	@Override
	public boolean equals(Object o) {
		if(o instanceof LogWriteable) {
			return this.hostname.equals(((LogWriteable) o).hostname) && this.username.equals(((LogWriteable) o).username);

		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return this.toString().hashCode();
	}


}
