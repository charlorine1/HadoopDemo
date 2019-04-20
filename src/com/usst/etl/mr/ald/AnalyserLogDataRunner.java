package com.usst.etl.mr.ald;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.usst.commom.EventLogConstants;
import com.usst.commom.GlobalConstants;
import com.usst.util.TimeUtil;

public class AnalyserLogDataRunner implements Tool {
	private static final Logger logger = Logger
			.getLogger(AnalyserLogDataMapper.class);
	private static Configuration conf = null;

	public static void main(String[] args) {
		try {
			ToolRunner.run(conf, new AnalyserLogDataRunner(), args);
		} catch (Exception e) {
			logger.error("执行日志解析job异常", e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		conf.set("fs.defaultFS", "hdfs://cdh01:8020");
		// conf.set("yarn.resourcemanager.hostname", "node1");
		conf.set("hbase.zookeeper.quorum", "cdh02");
		conf = HBaseConfiguration.create(conf);
	}

	@Override
	public int run(String[] arg) throws Exception {
		// arg 是main方法的入参
		// 往hbase导入哪天的数据 ,
		this.processArgs(conf, arg); // 往配置文件放入处理的时间

		Job job = Job.getInstance(conf, "analyser_logdata");

		// 设置本地提交job，集群运行，需要代码
		// File jarFile = EJob.createTempJar("target/classes");
		// ((JobConf) job.getConfiguration()).setJar(jarFile.toString());
		// 设置本地提交job，集群运行，需要代码结束

		job.setJarByClass(AnalyserLogDataRunner.class);
		job.setMapperClass(AnalyserLogDataMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Put.class);
		// 设置reducer配置
		// 1. 集群上运行，打成jar运行(要求addDependencyJars参数为true，默认就是true)
		// TableMapReduceUtil.initTableReducerJob(EventLogConstants.HBASE_NAME_EVENT_LOGS,
		// null, job);
		// 2. 本地运行，要求参数addDependencyJars为false
		TableMapReduceUtil.initTableReducerJob(EventLogConstants.HBASE_NAME_EVENT_LOGS, null, job, null, null, null,
				null, false);
		job.setNumReduceTasks(0);

		// 设置输入路径
		this.setJobInputPaths(job);
		return job.waitForCompletion(true) ? 0 : -1;

	}

	/**
	 * 设置输入路径
	 * 
	 * 
	 */
	private void setJobInputPaths(Job job) {
	    Configuration configuration = null ;
		FileSystem fs = null ;
		try {
			configuration = job.getConfiguration();
			String date = configuration.get(GlobalConstants.RUNNING_DATE_PARAMES); // 2019-04-20
			Path inputPath = new Path(
					"/log/" + TimeUtil.parseLong2String(TimeUtil.parseString2Long(date), "yyyyMMdd") + "/");
			fs = FileSystem.get(configuration);
			if(fs.exists(inputPath)) {
				//读取数据
				FileInputFormat.addInputPath(job, inputPath);
			}else {
				throw new RuntimeException("文件不存在:" + inputPath);
			}	
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("输入文件异常");
		}finally {
			//关闭连接
			try {
				if(fs != null ) {
					fs.close();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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
