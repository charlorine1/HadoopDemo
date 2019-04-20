package com.usst.transformer.mr.nu;

import org.apache.hadoop.hbase.mapreduce.TableMapper;

import com.usst.transformer.model.dim.StatsUserDimension;

public class NewInstallUserMapper extends TableMapper<StatsUserDimension, TimeOutputValue> {

}
