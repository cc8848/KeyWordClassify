package com.zyx.svm;

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;

import com.zyx.conf.CommonMethod;


import de.bwaldvogel.liblinear.Feature;
import de.bwaldvogel.liblinear.FeatureNode;
import de.bwaldvogel.liblinear.InvalidInputDataException;
import de.bwaldvogel.liblinear.Linear;
import de.bwaldvogel.liblinear.Model;
import de.bwaldvogel.liblinear.Parameter;
import de.bwaldvogel.liblinear.Problem;
import de.bwaldvogel.liblinear.SolverType;
import de.bwaldvogel.liblinear.Train;

public class GroupSVMClassify {
	public static class GroupSVMMapper extends Mapper<Object, Text, Text, Text>{
		private Text out_key = new Text();
		private Text out_value = new Text();
		
		private Problem problem;
		private Model model;
		private double bias;
		private String kw;
		private Feature[] x;
		
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String str_value = value.toString();
			
			double predict = 0;
			try {
				getTestInstance(str_value, bias);
				predict = Linear.predict(model, x);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				out_key.set(kw);
				int int_predict = (int)predict;
				out_value.set(int_predict + "");
				context.write(out_key, out_value);
			}
		}
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			Configuration conf = context.getConfiguration();
			String train_vector_file = conf.get("train_vector");
			
			int labels1_length = conf.getInt("labels1_length", 0);
			int labels2_length = conf.getInt("labels2_length", 0);
			int labels1[] = new int[labels1_length];
			int labels2[] = new int[labels2_length];
			for(int i = 0; i < labels1_length; i++){
				labels1[i] = conf.getInt("label1" + (i+1), -1);
			}
			for(int i = 0; i < labels2_length; i++){
				labels2[i] = conf.getInt("label2" + (i+1), -1);
			}
			
			bias = 1.0;
			
			FileSystem fs = FileSystem.get(URI.create(train_vector_file), conf);
			FSDataInputStream in = fs.open(new Path(train_vector_file));
			LineReader line_in = new LineReader(in, conf);
			try {
				problem = Train.readProblem(line_in, bias, labels1, labels2);
			} catch (InvalidInputDataException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			line_in.close();
			in.close();
			
			// s=4 C=1.0 get the highest score
			SolverType solvertype = SolverType.MCSVM_CS;
			double C = 1.0;
			
			double eps = 0.01;
			Parameter param = new Parameter(solvertype, C, eps);
			Linear.disableDebugOutput();
			model = Linear.train(problem, param);
		}
		
		private void getTestInstance(String line, double bias) throws Exception{
			 StringTokenizer itr = new StringTokenizer(line, "\t\n");
             kw = itr.nextToken();
             String str_index_count = itr.nextToken();
             
             String token;             
             StringTokenizer st = new StringTokenizer(str_index_count, " :\t\n");
             int n = problem.n;
             
             int m = st.countTokens() / 2;
             
             if (bias >= 0) {
                 x = new Feature[m + 1];
                 x[m] = new FeatureNode(n, bias);
             } else {
                 x = new Feature[m];
             }
             int indexBefore = 0;
             for (int j = 0; j < m; j++) {
                 token = st.nextToken();
                 int index = Integer.parseInt(token);
                 
                 if(index < 0 || index <= indexBefore){
                	 throw new Exception("train file index error");
                 }
                 // assert that indices are valid and sorted
                 indexBefore = index;

                 token = st.nextToken();
                 double value = Double.parseDouble(token);
                 x[j] = new FeatureNode(index, value);
             }
             
		}
	}
	
	public static void svm_classify(Configuration conf, String in, String out, int[] labels1, int[] labels2) throws IOException, InterruptedException, ClassNotFoundException{
		conf.setInt("labels1_length", labels1.length);
		conf.setInt("labels2_length", labels2.length);
		for(int i = 0; i < labels1.length; i++){
			conf.setInt("label1" + (i+1), labels1[i]);
		}
		for(int i = 0; i < labels2.length; i++){
			conf.setInt("label2" + (i+1), labels2[i]);
		}
	    Job job = new Job(conf, "SVMTest");
	    
	    job.setJarByClass(GroupSVMClassify.class);
	    job.setMapperClass(GroupSVMMapper.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(in));
	    FileOutputFormat.setOutputPath(job, new Path(out));
	    CommonMethod.runJob(job, conf, out);
	}
	
	public static int groups[][] = { 
		// decrease group number to speed up
		{1, 2, 3, 4},
		{5, 6, 7, 8},
		{9, 10, 11, 12},
		{13, 14, 15, 16, 33},
		{17, 18, 19, 20},
		{21, 22, 23, 24},
		{25, 26, 27, 28},
		{29, 30, 31, 32}
		
		// use this group can get highest score
//		{1,2,3},
//		{4,5,6},
//		{7,8,9},
//		{10,11,12},
//		{13,14,15},
//		{16,17,18},
//		{19,20,21},
//		{22,23,24},
//		{25,26,27},
//		{28,29,30},
//		{31,32,33}
		};
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 5) {
			System.err.println("Usage: com.zyx.svm.GroupSVMClassify (in:test_vector) (in:train_vector) (out) (stop_i) (stop_j)");
			System.exit(2);
		}
		String in = otherArgs[0];
		conf.set("train_vector", otherArgs[1]);
		
		if(!otherArgs[2].endsWith("/")){
			otherArgs[2] = otherArgs[2] + "/";
		}
		
		int s_i = Integer.parseInt(otherArgs[3]);
		int s_j = Integer.parseInt(otherArgs[4]);
		
		for(int i = 0; i < groups.length - 1; i++){
			for(int j = i + 1; j < groups.length; j++){
				if(i < s_i){
					continue;
				}
				if(i == s_i && j <= s_j){
					continue;
				}
				String out = otherArgs[2] + "groups" + i + "-" + j;
				svm_classify(conf, in, out, groups[i], groups[j]);
			}
		}
	}
}
