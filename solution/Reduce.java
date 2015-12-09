package solution;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		StringBuilder matches = new StringBuilder(values.next().toString());
		while (values.hasNext()) {
			matches.append(", ");
			matches.append(values.next());
		}
		matches.append(".");
		output.collect(new Text(key.toString() + ":"), new Text(matches.toString()));
	}
}