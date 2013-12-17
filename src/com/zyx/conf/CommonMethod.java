package com.zyx.conf;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

public class CommonMethod {
	public static Boolean isLabel(String tag){
		try{
			int num = Integer.parseInt(tag);
			if(num >= 1 && num <= ConfInfo.class_number){
				return true;
			}
			return false;
		} catch(NumberFormatException e){
			return false;
		}
	}
	
	public static void runJob(Job job, Configuration conf, String out) throws IOException, InterruptedException, ClassNotFoundException{
		int job_run_counter = 0;
		while(!job.waitForCompletion(true)){
			// wait to modify
			job.killJob(); 
			
			FileSystem fs = FileSystem.get(URI.create(out), conf);
		    if(fs.exists(new Path(out))){
		    	fs.delete(new Path(out), true);
		    }
		    fs.close();
		    job_run_counter++;
		    if(job_run_counter >= ConfInfo.job_attemp_times){
		    	System.exit(1);
		    }
		}
	}
}
