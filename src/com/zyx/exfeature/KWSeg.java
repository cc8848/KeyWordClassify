package com.zyx.exfeature;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import com.zyx.conf.CommonMethod;
import com.zyx.conf.ConfInfo;

public class KWSeg {
	public static class KWSegMapper extends Mapper<Object, Text, Text, Text>{
		private Text label = new Text();
		private Text feature = new Text();
		private IKSegmenter iks;
		
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
			if(itr.countTokens() == 2){
				String str_word = itr.nextToken();
				String str_label  = itr.nextToken();
				String str_feature = chineseSeg(str_word);
				label.set(str_label);
				if(str_label.startsWith("-")){
					feature.set(str_word + ConfInfo.kv_split_char + str_feature);
				} else{
					feature.set(str_feature);
				}
				context.write(label, feature);
			}
		}
		
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			Configuration conf = context.getConfiguration();
			Boolean spilt_op = conf.getBoolean("spilt_op", true);
			iks = new IKSegmenter(spilt_op);
		}


		private String chineseSeg(String value) throws IOException{
			byte[] bt = value.getBytes();
			InputStream ip = new ByteArrayInputStream(bt);
			Reader reader = new InputStreamReader(ip);
			iks.reset(reader);
			Lexeme t;
			StringBuffer sb_result = new StringBuffer("");
			while((t = iks.next()) != null){
				sb_result.append(t.getLexemeText());
				sb_result.append(" ");
			}
			String result = new String(sb_result);
			return result;
		}
	}
	
	public static void wordSeg(Configuration conf, String in, String out, Boolean op) throws IOException, InterruptedException, ClassNotFoundException{
		conf.setBoolean("spilt_op", op);
	    
	    Job job = new Job(conf, "KWSeg");
	    job.setJarByClass(KWSeg.class);
	    job.setMapperClass(KWSegMapper.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(in));
	    FileOutputFormat.setOutputPath(job, new Path(out));
	    CommonMethod.runJob(job, conf, out);
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 3) {
	    	System.err.println("Usage: com.zyx.exfeature.KWSeg (in) (out) (true|false)");
	    	System.exit(2);
	    }
	    if(otherArgs[2].compareTo("true") != 0  && otherArgs[2].compareTo("false") != 0){
	    	System.err.println("Third arg must be 'true' or 'false'");
	    	System.exit(2);
	    }
	    Boolean spilt_option = Boolean.parseBoolean(otherArgs[2]);
	    wordSeg(conf, otherArgs[0], otherArgs[1], spilt_option);
	}
}
