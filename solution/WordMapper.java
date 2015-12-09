package solution;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WordMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	private Set<String> patternsToFilterBy = new HashSet<String>();

	public void configure(JobConf job) {

		Path[] patternsFiles = new Path[0];
		try {
			patternsFiles = DistributedCache.getLocalCacheFiles(job);
			for (Path patternsFile : patternsFiles) {
				parseSkipFile(patternsFile);
				}
		} catch (IOException ioe) {
			System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
		}
	}

	private void parseSkipFile(Path patternsFile) {
		BufferedReader fis = null;
		try {
			fis = new BufferedReader(new FileReader(patternsFile.toString()));
			String pattern = null;
			while ((pattern = fis.readLine()) != null) {
				patternsToFilterBy.add(pattern);
			}
		} catch (IOException ioe) {
			System.err.println("Caught exception while parsing the cached file '"+ patternsFile + "' : "+ StringUtils.stringifyException(ioe));
		}
		finally {
			try {fis.close();} catch (IOException e) {}
		}
	}

	public void map(LongWritable key, Text value,OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

		String word = value.toString();

		for (String pattern : patternsToFilterBy) {
			if(pattern.length() == word.length() && pattern.endsWith(word.substring(word.length() - 1))) {
				output.collect(new Text(pattern), new Text(word));
			}
		}
	}
}
