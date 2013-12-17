package com.zyx.svm;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.zyx.conf.CommonMethod;
import com.zyx.conf.ConfInfo;


public class VoteResult {
	public static class VoteMapper extends Mapper<Object, Text, Text, Text>{
		private Text out_key = new Text();
		private Text out_value = new Text();
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			StringTokenizer itr = new StringTokenizer(value.toString(), "\t\n");
			if(itr.countTokens() != 2){
				return;
			}
			String kw = itr.nextToken();
			String label = itr.nextToken();
			out_key.set(kw);
			out_value.set(label);
			context.write(out_key, out_value);
		}
	}
	public static class VoteReducer extends Reducer<Text, Text, Text, Text>{
		private Text out_value = new Text();
		private HashMap<String, Integer> count_label = new HashMap<String, Integer>(ConfInfo.class_number);
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			countLabel(values);
			String result = getMost();
			out_value.set(result);
			context.write(key, out_value);
		}
		
		private void countLabel(Iterable<Text> values){
			count_label.clear();
			for(Text val : values){
				String str_val = val.toString();
				if(count_label.containsKey(str_val)){
					int src_value = count_label.get(str_val);
					count_label.put(str_val, src_value + 1);
				} else {
					count_label.put(str_val, 1);
				}
			}
		}
		
		private String getMost(){
			String result_label = "-";
			int max_value = Integer.MIN_VALUE;
			Iterator<Entry<String, Integer> > itr = count_label.entrySet().iterator();
			while(itr.hasNext()){
				Entry<String, Integer> entry = itr.next();
				int value = entry.getValue();
				if(value >= max_value){
					max_value = value;
					result_label = entry.getKey();
				}
			}
			return result_label;
		}
	}
	
	public static class MyPartition extends Partitioner<Text, Text>{

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			// TODO Auto-generated method stub
			String str_key = key.toString();
			int hashcode = str_key.hashCode();
			if(str_key.length() <= 1){
				return numPartitions - 1 - hashcode % numPartitions;
			}
			String last_letter = str_key.substring(str_key.length() - 1);
			int last_hash_code = last_letter.hashCode();
			int result = (hashcode + (str_key.length() + last_hash_code )* ConfInfo.class_number) % numPartitions;
			return numPartitions - 1 - result;
		}
		
	}
	
	public static void voteResult(Configuration conf, String in_dir, String out) throws IOException, InterruptedException, ClassNotFoundException{
		Job job = new Job(conf, "VoteResult");
	    
	    job.setJarByClass(VoteResult.class);
	    job.setMapperClass(VoteMapper.class);
	    job.setReducerClass(VoteReducer.class);
	    job.setPartitionerClass(MyPartition.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    int len = GroupSVMClassify.groups.length;
	    for(int i = 0; i < len - 1; i++){
	    	for(int j = i + 1; j < len; j++){
	    		String in_path = in_dir + "groups" + i + "-" + j + "/part-r-00000";
	    		FileInputFormat.addInputPath(job, new Path(in_path));
	    	}
	    }
	    
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
			System.err.println("Usage: com.zyx.svm.VoteResult (in: dir) (out)");
			System.exit(2);
		}
		if(!otherArgs[0].endsWith("/")){
			otherArgs[0] = otherArgs[0] + "/";
		}
		voteResult(conf, otherArgs[0], otherArgs[1]);
	}
}
