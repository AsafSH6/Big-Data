package solution;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class Reduce extends MapReduceBase implements Reducer<LogWriteable, IntWritable, LogWriteable, IntWritable> {
	static enum Counters{ OUTPUT_KEYS } 
	
	public void reduce(LogWriteable key, Iterator<IntWritable> values, OutputCollector<LogWriteable, IntWritable> output, Reporter reporter) throws IOException {
		int sum = 0;
		reporter.incrCounter(Counters.OUTPUT_KEYS, 1);
		while (values.hasNext()) {
			sum += values.next().get();
			
		}
		output.collect(key, new IntWritable(sum));
	}
}