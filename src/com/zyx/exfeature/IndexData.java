package com.zyx.exfeature;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

import com.zyx.conf.CommonMethod;
import com.zyx.conf.ConfInfo;



public class IndexData {
	private static void countFeatures(HashMap<String, Integer> feature_count, String features){
		feature_count.clear();
		StringTokenizer itr = new StringTokenizer(features, " ");
		while(itr.hasMoreTokens()){
			String feature = itr.nextToken();
			if(feature_count.containsKey(feature)){
				int src_value = feature_count.get(feature);
				feature_count.put(feature, src_value + 1);
			} else {
				feature_count.put(feature, 1);
			}
		}
	}
	
	private static void indexFeatures(HashMap<String, Integer> feature_count, HashMap<Integer, Integer> index_count, HashMap<String, Integer> feature_index){
		index_count.clear();
		Iterator<Entry<String, Integer> > itr = feature_count.entrySet().iterator();
		while(itr.hasNext()){
			Entry<String, Integer> entry = itr.next();
			String key = entry.getKey();
			if(feature_index.containsKey(key)){
				int count = entry.getValue();
				int index = feature_index.get(key);
				index_count.put(index, count);
			}
		}
	}
	
	private static String getIndexFeatures(HashMap<Integer, Integer> index_count){
		if(index_count.size() == 0){
			return "";
		}
		StringBuffer sb_result = new StringBuffer("");
		
		List<Entry<Integer, Integer> > list_index_count = new ArrayList<Entry<Integer,Integer>>(index_count.entrySet());
		Collections.sort(list_index_count, new Comparator<Entry<Integer, Integer> >() {

			@Override
			public int compare(Entry<Integer, Integer> o1,
					Entry<Integer, Integer> o2) {
				// TODO Auto-generated method stub
				return o1.getKey() - o2.getKey();
			}
			
		});
		for(Entry<Integer, Integer> entry : list_index_count){
			int index = entry.getKey();
//			int count = entry.getValue();
//			sb_result.append(index + ConfInfo.kv_split_char + count + " ");
			sb_result.append(index + ConfInfo.kv_split_char + 1 + " ");
		}
		return new String(sb_result);
	}
	
	private static void readCount(HashMap<String, Integer> feature_index, LineReader line_in) throws IOException{
		Text line = new Text();
		
		String kw = "";
		feature_index.clear();
		int counter = 0;
		while(line_in.readLine(line) > 0) {
			++counter;
			StringTokenizer itr = new StringTokenizer(line.toString(), "\t\n");
			kw = itr.nextToken();
			itr.nextToken();
			feature_index.put(kw, counter);
		}
	}
	
	
	public static class IndexTrainMapper extends Mapper<Object, Text, Text, Text>{
		private Text out_key = new Text();
		private Text out_value = new Text();
		private HashMap<String, Integer> feature_index = new HashMap<String, Integer>();
		private HashMap<String, Integer> feature_count = new HashMap<String, Integer>();
		private HashMap<Integer, Integer> index_count  = new HashMap<Integer, Integer>();
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String str_value = value.toString();
			if(str_value.startsWith("-")){
				return;
			}
			StringTokenizer itr = new StringTokenizer(str_value, "\t\n");
			String label = itr.nextToken();
			String features = itr.nextToken();
			countFeatures(feature_count, features);
			indexFeatures(feature_count, index_count, feature_index);
			String str_index_count = getIndexFeatures(index_count);
			if(str_index_count.compareTo("") == 0){
				return;
			}
			out_key.set(label);
			out_value.set(str_index_count);
			context.write(out_key, out_value);
			
		}
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			Configuration conf = context.getConfiguration();
			String uri = conf.get("chi");
			
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			FSDataInputStream in = fs.open(new Path(uri));
			LineReader line_in = new LineReader(in, conf);
			
			readCount(feature_index, line_in);
			line_in.close();
			in.close();
		}
		
	}
	
	public static class IndexTestMapper extends Mapper<LongWritable, Text, Text, Text>{
		private Text out_key = new Text();
		private Text out_value = new Text();
		
		private HashMap<String, Integer> feature_index = new HashMap<String, Integer>();
		private HashMap<String, Integer> feature_count = new HashMap<String, Integer>();
		private HashMap<Integer, Integer>  index_count = new HashMap<Integer, Integer>();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String str_value = value.toString();
			if(!str_value.startsWith("-")){
				return;
			}
			StringTokenizer itr = new StringTokenizer(str_value, ":\t\n");
			if(itr.countTokens() != 3){
				return;
			}
			itr.nextToken();
			String kw = itr.nextToken();
			String features = itr.nextToken();
			
			countFeatures(feature_count, features);
			indexFeatures(feature_count, index_count, feature_index);
			String str_index_count = getIndexFeatures(index_count);
			
			if(str_index_count.compareTo("") == 0){
				return;
			}
			out_key.set(kw);
			out_value.set(str_index_count);
			context.write(out_key, out_value);
		}
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			Configuration conf = context.getConfiguration();
			String uri = conf.get("chi");
			
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			FSDataInputStream in = fs.open(new Path(uri));
			LineReader line_in = new LineReader(in, conf);
			
			readCount(feature_index, line_in);
			line_in.close();
			in.close();
		}
	}
	
	public static void indexTrain(Configuration conf, String in_features, String in_chi, String out) throws IOException, InterruptedException, ClassNotFoundException{
		conf.set("chi", in_chi);
		Job job = new Job(conf, "IndexTrain");
	    
	    job.setJarByClass(IndexData.class);
	    job.setMapperClass(IndexTrainMapper.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(in_features));
	    FileOutputFormat.setOutputPath(job, new Path(out));
	    CommonMethod.runJob(job, conf, out);
	}
	
	public static void indexTest(Configuration conf, String in_features, String in_chi, String out) throws IOException, InterruptedException, ClassNotFoundException{
		conf.set("chi", in_chi);
		Job job = new Job(conf, "IndexTest");
	    
	    job.setJarByClass(IndexData.class);
	    job.setMapperClass(IndexTestMapper.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(in_features));
	    FileOutputFormat.setOutputPath(job, new Path(out));
	    
	    CommonMethod.runJob(job, conf, out);
	}
}
