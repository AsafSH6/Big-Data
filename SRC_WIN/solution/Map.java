package solution;

import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class Map extends MapReduceBase implements Mapper<LongWritable, Text, LogWriteable, IntWritable> {

	public void map(LongWritable key, Text value,OutputCollector<LogWriteable, IntWritable> output, Reporter reporter) throws IOException {

		String[] lines = value.toString().split(" ");
		String[] relevantLines = new String[4];
		for(int i=1, j=0; i < lines.length; i++) {
			if(!lines[i].equals("") && !lines[i].equals("PM") && !lines[i].equals("AM")) {
				relevantLines[j] = lines[i];
				++j;
			}
		}
		output.collect(new LogWriteable(relevantLines[0].split("/")[2], relevantLines[1], relevantLines[2], relevantLines[3]), new IntWritable(1));

	}
}
