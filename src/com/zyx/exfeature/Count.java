package com.zyx.exfeature;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.zyx.conf.CommonMethod;



public class Count {
	public static class CountMapper extends Mapper<Object, Text, Text, IntWritable>{
		private Text out_key = new Text();
		private final static IntWritable one = new IntWritable(1);
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String str_value = value.toString();
			if(str_value.startsWith("-")){
				return;
			}
			StringTokenizer line_itr = new StringTokenizer(str_value, "\t");
			if(line_itr.countTokens() == 2){
				String label = line_itr.nextToken();
				if(!CommonMethod.isLabel(label)){
					return;
				}
				String split_words = line_itr.nextToken();
				StringTokenizer words_itr = new StringTokenizer(split_words, " ");
				while(words_itr.hasMoreTokens()){
					String word = words_itr.nextToken();
					out_key.set(word);
					context.write(out_key, one);
				}
			}
		}
		
	}
	
	public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable out_sum = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum = 0;
			for(IntWritable val : values){
				sum += val.get();
			}
			out_sum.set(sum);
			context.write(key, out_sum);
		}
		
	}
	public static void count(Configuration conf, String in, String out) throws IOException, InterruptedException, ClassNotFoundException{
		Job job = new Job(conf, "count");
	    job.setJarByClass(Count.class);
	    job.setMapperClass(CountMapper.class);
	    job.setCombinerClass(CountReducer.class);
	    job.setReducerClass(CountReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(in));
	    FileOutputFormat.setOutputPath(job, new Path(out));
	    CommonMethod.runJob(job, conf, out);
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: com.zyx.exfeature.count (in) (out)");
	      System.exit(2);
	    }
	    count(conf, otherArgs[0], otherArgs[1]);
	    
	}

}
