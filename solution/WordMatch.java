package solution;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WordMatch extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), WordMatch.class);
		conf.setJobName("WordMatcher");

		conf.setOutputKeyClass(LogWriteable.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(WordMapper.class);
		//	conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordMatch(), args);
		System.exit(res);
	}
}