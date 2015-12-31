package solution;

import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class Map extends MapReduceBase implements Mapper<LongWritable, Text, LogWriteable, IntWritable> {

	public void map(LongWritable key, Text value,OutputCollector<LogWriteable, IntWritable> output, Reporter reporter) throws IOException {

		String[] lines = value.toString().split(" ");
		String[] relevantLines = new String[2];
		for(int i=0, int j=0; i < lines.length; i++) {
			if(!lines[i].equals("")) {
				if(j == 1)
					relevantLines[0] = lines[i];
				else if(j == 5)
					relevantLines[1] = lines[i];
				++j;
			}
		}
				output.collect(new LogWriteable(relevantLines[0].split("/")[2], relevantLines[1]), new IntWritable(1));
	}
}
