package com.mr.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DescKey extends Configured implements Tool {

	public static class MyMapper extends
			Mapper<LongWritable, Text, IntWritable, IntWritable> {

		protected void map(
				LongWritable key,
				Text value,
				org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
				throws java.io.IOException, InterruptedException {

			for (int i = 0; i < 10; i++) {
				context.write(new IntWritable(i), new IntWritable(i));
			}

		};
	}

	public static class MyReducer extends
			Reducer<IntWritable, IntWritable, NullWritable, IntWritable> {
		protected void reduce(
				IntWritable arg0,
				java.lang.Iterable<IntWritable> arg1,
				org.apache.hadoop.mapreduce.Reducer<IntWritable, IntWritable, NullWritable, IntWritable>.Context arg2)
				throws java.io.IOException, InterruptedException {
		
			for(IntWritable iw:arg1){
				arg2.write(NullWritable.get(), iw);
			}
		};
	}
	
	public static class MyDescKey extends WritableComparator{
		
		public MyDescKey() {
			// TODO Auto-generated constructor stub
			//必需有的
			super(IntWritable.class,true);
		}
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			// TODO Auto-generated method stub
			
			return super.compare(b, a);
		}
	}

	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "desc key...");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		job.setInputFormatClass(TextInputFormat.class);

		job.setJarByClass(DescKey.class);

		job.setMapperClass(MyMapper.class);

		job.setMapOutputKeyClass(IntWritable.class);

		job.setMapOutputValueClass(IntWritable.class);
		
		job.setSortComparatorClass(MyDescKey.class);
		
		job.setReducerClass(MyReducer.class);
		
		job.setOutputKeyClass(NullWritable.class);
		
		job.setOutputValueClass(IntWritable.class);
		
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

		int exitCode = ToolRunner.run(new DescKey(), args);
		
		System.exit(exitCode);
	}

}
