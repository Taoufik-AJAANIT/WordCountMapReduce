package com.malsolo.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountDriver {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] myArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (myArgs.length != 2) {
			System.err.println("Usage: WordCountDriver <input path> <output path>");
			System.exit(-1);
		}
		Job job = Job.getInstance(conf, "Classic WordCount");
		job.setJarByClass(WordCountDriver.class);
		
		FileInputFormat.addInputPath(job, new Path(myArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(myArgs[1]));
		
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}