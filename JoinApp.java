package com.mr.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JoinApp extends Configured implements Tool {

	public static class MyMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		protected void map(
				LongWritable key,
				Text value,
				org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>.Context context)
				throws java.io.IOException, InterruptedException {

			String[] splits = value.toString().split(" ");
			
			context.write(new Text(splits[0]), new Text(splits[1]));

		};
	}

	public static class MyReducer extends
			Reducer<Text, Text, Text, Text> {
		protected void reduce(
				Text arg0,
				java.lang.Iterable<Text> arg1,
				org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context arg2)
				throws java.io.IOException, InterruptedException {
		
			StringBuilder sb = new StringBuilder();
			
			for(Text iw:arg1){
				sb.append(iw.toString()+",");
			}
			
			arg2.write(arg0, new Text(sb.toString()));
		};
	}
	

	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "join app...");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		job.setInputFormatClass(TextInputFormat.class);

		job.setJarByClass(JoinApp.class);

		job.setMapperClass(MyMapper.class);

		job.setMapOutputKeyClass(Text.class);

		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(MyReducer.class);
		
		job.setOutputKeyClass(Text.class);
		
		job.setOutputValueClass(Text.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true)?0:1;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		int exitCode = ToolRunner.run(new JoinApp(), args);
		
		System.exit(exitCode);
	}

}
