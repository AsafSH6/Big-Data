package solution;

import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class Map extends MapReduceBase implements Mapper<LongWritable, Text, LogWriteable, IntWritable> {

	public void map(LongWritable key, Text value,OutputCollector<LogWriteable, IntWritable> output, Reporter reporter) throws IOException {

		String[] lines = value.toString().split(" ");
		String[] relevantLines = new String[2];
		relevantLines[0] = lines[4];
		relevantLines[1] = lines[31];
				output.collect(new LogWriteable(relevantLines[0].split("/")[2], relevantLines[1], new IntWritable(1));
	}
}
