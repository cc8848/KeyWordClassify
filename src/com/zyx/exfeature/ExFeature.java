package com.zyx.exfeature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

public class ExFeature {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: com.zyx.exfeature.ExFeature (in:seg) (out:dir)");
			System.exit(2);
		}
		if(!otherArgs[1].endsWith("/")){
			otherArgs[1] = otherArgs[1] + "/";
		}
		
		String seg_out = otherArgs[0];
		String count_out = otherArgs[1] + "count";
		String train_out = otherArgs[1] + "train_data";
		String test_out  = otherArgs[1] + "test_data";
		
		String count_in = count_out + "/part-r-00000";
		
		Count.count(conf, seg_out, count_out);
		IndexData.indexTrain(conf, seg_out, count_in, train_out);
		IndexData.indexTest(conf, seg_out,  count_in, test_out);
	}
}
