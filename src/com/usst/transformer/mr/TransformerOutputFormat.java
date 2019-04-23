package com.usst.transformer.mr;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.usst.commom.GlobalConstants;
import com.usst.commom.KpiType;
import com.usst.transformer.model.dim.base.BaseDimension;
import com.usst.transformer.model.value.BaseStatsValueWritable;
import com.usst.transformer.service.IDimensionConverter;
import com.usst.transformer.service.imp.DimensionConverterImpl;
import com.usst.util.JdbcManager;

public class TransformerOutputFormat extends OutputFormat<BaseDimension, BaseStatsValueWritable> {
	private static final Logger logger = Logger.getLogger(TransformerOutputFormat.class);

	@Override
	public void checkOutputSpecs(JobContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		// 检测输出空间，输出到mysql不用检测
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		 return new FileOutputCommitter(FileOutputFormat.getOutputPath(context), context);
	}

	/**
	 * 从reduce的暑促和端获取数据，让后通过改读取器读到数据，往数据库里面写数据， 该方法是得到读取器，读取器里面封装了对数据的操作
	 * 
	 */
	@Override
	public RecordWriter<BaseDimension, BaseStatsValueWritable> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		Connection conn = null;
		IDimensionConverter converter = new DimensionConverterImpl();
		try {
			conn = JdbcManager.getConnection(conf, GlobalConstants.WAREHOUSE_OF_REPORT);
			conn.setAutoCommit(false);
		} catch (Exception e) {
			logger.error("获取数据库连接失败", e);
			throw new IOException("获取数据库连接失败", e);
		}

		return new TransformerRecordWriter(conn, conf, converter);
	}

	public class TransformerRecordWriter extends RecordWriter<BaseDimension, BaseStatsValueWritable> {
		private Connection conn = null;
		private Configuration conf = null;
		private IDimensionConverter converter = null;
		private Map<KpiType, PreparedStatement> map = new HashMap<KpiType, PreparedStatement>();
		private Map<KpiType, Integer> batch = new HashMap<KpiType, Integer>();

		/**
		 * 执行完一次reduce（遍历values），所以才执行close
		 */
		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            try {
                for (Map.Entry<KpiType, PreparedStatement> entry : this.map.entrySet()) {
                    entry.getValue().executeBatch();
                }
            } catch (SQLException e) {
                logger.error("执行executeUpdate方法异常", e);
                throw new IOException(e);
            } finally {
                try {
                    if (conn != null) {
                        conn.commit(); // 进行connection的提交动作
                    }
                } catch (Exception e) {
                    // nothing
                } finally {
                    for (Map.Entry<KpiType, PreparedStatement> entry : this.map.entrySet()) {
                        try {
                            entry.getValue().close();
                        } catch (SQLException e) {
                            // nothing
                        }
                    }
                    if (conn != null)
                        try {
                            conn.close();
                        } catch (Exception e) {
                            // nothing
                        }
                }
            }
		}

		/**
		 * 当reduce任务输出数据是，由计算框架自动调用。把reducer输出的数据写到mysql中，reduce每次执行一次context.write()执行改write()
		 * 里面封装输出数据的方法
		 */
		@Override
		public void write(BaseDimension key, BaseStatsValueWritable value) throws IOException, InterruptedException {
			if (key == null || value == null) {
				return;
			}

			try {
				KpiType kpi = value.getKpi();
				PreparedStatement pstmt = null;// 每一个pstmt对象对应一个sql语句
				int count = 1;// sql语句的批处理，一次执行10
				if (map.get(kpi) == null) {
					// 使用kpi进行区分，返回sql保存到config中
					pstmt = this.conn.prepareStatement(conf.get(kpi.name));
					map.put(kpi, pstmt);
				} else {
					pstmt = map.get(kpi);
					count = batch.get(kpi);
					count++;
				}
				batch.put(kpi, count); // 批量次数的存储

				String collectorName = conf.get(GlobalConstants.OUTPUT_COLLECTOR_KEY_PREFIX + kpi.name);
				Class<?> clazz = Class.forName(collectorName);
				IOutputCollector collector = (IOutputCollector) clazz.newInstance();
				collector.collect(conf, key, value, pstmt, converter);
				if (count % Integer.valueOf(
						conf.get(GlobalConstants.JDBC_BATCH_NUMBER, GlobalConstants.DEFAULT_JDBC_BATCH_NUMBER)) == 0) {
					pstmt.executeBatch();
					conn.commit();
					batch.put(kpi, 0); // 对应批量计算删除
				}

			} catch (Throwable e) {
				logger.error("在writer中写数据出现异常", e);
				throw new IOException(e);
			}
		}

		public TransformerRecordWriter(Connection conn, Configuration conf, IDimensionConverter converter) {
			super();
			this.conn = conn;
			this.conf = conf;
			this.converter = converter;
		}

	}

}
