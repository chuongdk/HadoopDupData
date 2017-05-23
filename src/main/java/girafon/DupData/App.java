package girafon.DupData;
 


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

 

import org.apache.hadoop.mapreduce.lib.input.*;


// Using Hadoop MapReduce to duplicate transactions

public class App extends Configured implements Tool {

	
	private int numberReducers = 2;
	
	final long DEFAULT_SPLIT_SIZE = 128  * 1024 * 1024;   
	final long DEFAULT_DATA_SIZE = 2 * 1024 * 1024;  // size of a data block
	final long DEFAULT_CANDIDATE_SIZE = 2  * 1024 * 1024; // size of a candidate block   
	

	Configuration setupConf(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.set("input", args[0]);  // input location
		conf.setInt("duplication", Integer.valueOf(args[1]));  // number of duplication
		conf.set("output", args[2]);  // output location		
		return conf;
	}

 
	private Path getOutputPath(Configuration conf) {
		String sep = System.getProperty("file.separator");
		return new Path(conf.get("output"));
	}
 
	private Path getInputPath(Configuration conf) {
		return new Path(conf.get("input"));
	}	
	
	Job setupJob(Configuration conf) throws Exception {
		Job job = Job.getInstance(conf, "Duplicate data");
		job.setJarByClass(App.class);
		job.setMapperClass(MapDuplicate.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, getInputPath(conf));
		FileOutputFormat.setOutputPath(job, getOutputPath(conf));// output path for iteration 1 is: output/1
		
		return job;
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = setupConf(args);
		Job job = setupJob(conf);			 
		job.waitForCompletion(true);	
		
		return 1;
	}
		

	public static void main(String[] args) throws Exception {
		
		long beginTime = System.currentTimeMillis();
		int exitCode = ToolRunner.run(new App(), args);
		long endTime = System.currentTimeMillis();
		
		System.out.println("input : " + args[0]);
		System.out.println("duplication : " + args[1]);
		System.out.println("output : " + args[2]);
		// output = input + "/" + duplication
		
		System.out.println("Total time : " + (endTime - beginTime)/1000 + " seconds.");		
				
		System.exit(exitCode);
		

	}

}




