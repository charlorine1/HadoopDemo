package com.usst.transformer.mr.au;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import com.usst.commom.GlobalConstants;
import com.usst.transformer.model.dim.StatsUserDimension;
import com.usst.transformer.model.dim.base.BaseDimension;
import com.usst.transformer.model.value.BaseStatsValueWritable;
import com.usst.transformer.model.value.reduce.MapWritableValue;
import com.usst.transformer.mr.IOutputCollector;
import com.usst.transformer.service.IDimensionConverter;

public class ActiveUserCollector implements IOutputCollector {

    @Override
	public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt,
			IDimensionConverter converter) throws SQLException, IOException {
        // 进行强制后获取对应的值
        StatsUserDimension statsUser = (StatsUserDimension) key;
        IntWritable activeUserValue = (IntWritable) ((MapWritableValue) value).getValue().get(new IntWritable(-1));

        // 进行参数设置
        int i = 0;
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getStatsCommon().getPlatform()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getStatsCommon().getDate()));
        pstmt.setInt(++i, activeUserValue.get());
        pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
        pstmt.setInt(++i, activeUserValue.get());

        // 添加到batch中
        pstmt.addBatch();

	}

}
