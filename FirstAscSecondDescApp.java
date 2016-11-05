package com.mr.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FirstAscSecondDescApp extends Configured implements Tool {

	public static class MyPair implements WritableComparable<MyPair> {

		private int first;

		private int second;

		// 必需有无参的构造函数

		public int getFirst() {
			return first;
		}

		public void setFirst(int first) {
			this.first = first;
		}

		public int getSecond() {
			return second;
		}

		public void setSecond(int second) {
			this.second = second;
		}

		@Override
		public void readFields(DataInput arg0) throws IOException {
			// TODO Auto-generated method stub
			first = arg0.readInt();

			second = arg0.readInt();
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			// TODO Auto-generated method stub
			arg0.writeInt(first);
			arg0.writeInt(second);
		}

		@Override
		public int compareTo(MyPair o) {

			// first升序 second降序
			
			if (first == o.getFirst()){
				return o.getSecond() - second;
			}

			return this.first - o.getFirst();
		}

	}

	public static class MyMapper extends
			Mapper<LongWritable, Text, MyPair, NullWritable> {

		protected void map(
				LongWritable key,
				Text value,
				org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, MyPair, NullWritable>.Context context)
				throws java.io.IOException, InterruptedException {

			for (int i = 0; i < 10; i++) {

				for (int j = 10; j >= 0; j--) {
					MyPair mp = new MyPair();

					mp.setFirst(i);

					mp.setSecond(j);

					context.write(mp, NullWritable.get());
				}
			}

		};
	}

	public static class MyReducer extends
			Reducer<MyPair, NullWritable, IntWritable, IntWritable> {
		protected void reduce(
				MyPair arg0,
				java.lang.Iterable<NullWritable> arg1,
				org.apache.hadoop.mapreduce.Reducer<MyPair, NullWritable, IntWritable, IntWritable>.Context arg2)
				throws java.io.IOException, InterruptedException {

			arg2.write(new IntWritable(arg0.getFirst()), new IntWritable(arg0.getSecond()));
		};
	}
	

	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "desc key...");

		FileInputFormat.addInputPath(job, new Path(args[0]));

		job.setInputFormatClass(TextInputFormat.class);

		job.setJarByClass(FirstAscSecondDescApp.class);

		job.setMapperClass(MyMapper.class);

		job.setMapOutputKeyClass(MyPair.class);

		job.setMapOutputValueClass(NullWritable.class);

		job.setReducerClass(MyReducer.class);

		job.setOutputKeyClass(IntWritable.class);

		job.setOutputValueClass(IntWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		int exitCode = ToolRunner.run(new FirstAscSecondDescApp(), args);

		System.exit(exitCode);
	}

}
