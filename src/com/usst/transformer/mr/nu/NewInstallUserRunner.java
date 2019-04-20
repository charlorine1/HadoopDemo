package com.usst.transformer.mr.nu;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.usst.commom.GlobalConstants;
import com.usst.util.TimeUtil;


/**
 * 1、从hbase里面读数据    然后通过不通维度进行处理
 * 
 * 
 * */
public class NewInstallUserRunner implements Tool {
    private static Configuration conf = null ;

	@Override
	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public void setConf(Configuration conf) {
        conf.addResource("output-collector.xml");
        conf.addResource("query-mapping.xml");
        conf.addResource("transformer-env.xml");
        conf.set("fs.defaultFS", "hdfs://node1:8020");
//    	conf.set("yarn.resourcemanager.hostname", "node3");
    	conf.set("hbase.zookeeper.quorum", "node4");
        conf = HBaseConfiguration.create(conf);

	}

	@Override
	public int run(String[] args) throws Exception {
		//获取设置要处理的日期
		this.processArgs(conf, args);
		Job job = Job.getInstance(conf, "analyser_hbase_date");
		
		job.setJarByClass(NewInstallUserRunner.class);
		
		TableMapReduceUtil.initTableMapperJob(
				                       initScan(), 
				                       mapper,
				                       outputKeyClass, 
				                       outputValueClass,
				                       job
				                       false);
		
		job.setMapOutputKeyClass(theClass);
		job.setMapOutputValueClass(theClass);
		
		job.setReducerClass(cls);
		job.setOutputKeyClass(theClass);
		job.setOutputValueClass(theClass);
		job.setOutputFormatClass(cls);
		
		
		
		
		
		
		
		return 0;
	}
	
	
	public static void main(String [] args) {
		int result = 0 ;
		try {
		     result = ToolRunner.run(conf, new NewInstallUserRunner(), args);
		     if(result == 0) {
		    	 
		     }
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void processArgs(Configuration conf, String[] args) {
		String date = "";
		if (args.length > 1) {
			for (int i = 0; i < args.length; i++) {
				if (StringUtils.equals(args[i], "-d")) {
					date = args[i + 1];
				}
			}
			if (TimeUtil.isValidateRunningDate(date)) {
				conf.set(GlobalConstants.RUNNING_DATE_PARAMES, date);
				return;
			}
		}
		// 当前时间前一天
		date = TimeUtil.getYesterday();
		conf.set(GlobalConstants.RUNNING_DATE_PARAMES, date);
	}



}
