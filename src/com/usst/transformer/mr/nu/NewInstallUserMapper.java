package com.usst.transformer.mr.nu;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Mapper;

import com.usst.transformer.model.dim.StatsUserDimension;
import com.usst.transformer.model.value.map.TimeOutputValue;

public class NewInstallUserMapper extends TableMapper<StatsUserDimension, TimeOutputValue> {

	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			Mapper<ImmutableBytesWritable, Result, StatsUserDimension, TimeOutputValue>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.map(key, value, context);
	}

}
