package com.usst.transformer.mr.au;

import java.io.IOException;
import java.util.List;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.usst.commom.DateEnum;
import com.usst.commom.EventLogConstants;
import com.usst.commom.KpiType;
import com.usst.transformer.model.dim.StatsCommonDimension;
import com.usst.transformer.model.dim.StatsUserDimension;
import com.usst.transformer.model.dim.base.BrowserDimension;
import com.usst.transformer.model.dim.base.DateDimension;
import com.usst.transformer.model.dim.base.KpiDimension;
import com.usst.transformer.model.dim.base.PlatformDimension;
import com.usst.transformer.model.value.map.TimeOutputValue;

public class ActiveUserMapper extends TableMapper<StatsUserDimension, TimeOutputValue> {
	private static final Logger logger = Logger.getLogger(ActiveUserMapper.class);
	private StatsUserDimension statsUserDimension = new StatsUserDimension();
	private TimeOutputValue timeOutputValue = new TimeOutputValue();
	private BrowserDimension defaultBrowser = new BrowserDimension("", ""); // 默认的browser对象
	private byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);
	private KpiDimension activeUserKpi = new KpiDimension(KpiType.ACTIVE_USER.name);// 代表用户分析模块的统计
	private KpiDimension activeUserOfBrowserKpi = new KpiDimension(KpiType.BROWSER_NEW_INSTALL_USER.name);// 浏览器分析模块的统计

	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {
		String uuid = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_UUID)));
		String serverTime = Bytes
				.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)));
		String platform = Bytes
				.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)));

		System.out.println(uuid + "-" + serverTime + "-" + platform);
		if (StringUtils.isBlank(uuid) || StringUtils.isBlank(serverTime) || StringUtils.isBlank(platform)
				|| StringUtils.isNumeric(serverTime)) {
			logger.warn("uuid&serviceTIme&platform 不能为空，切serviceTime必须是时间戳");
		}

		timeOutputValue.setId(uuid);
		long serviceTime = Long.parseLong(serverTime.trim());

		// DateDimension dateDimension = new DateDimension();
		DateDimension dateDimensionList = DateDimension.buildDate(serviceTime, DateEnum.DAY);

		// 获取browser name和browser version
		String browser = Bytes
				.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME)));
		String browserVersion = Bytes
				.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION)));
		// 进行browser的维度信息构建
		List<BrowserDimension> browsers = BrowserDimension.buildList(browser, browserVersion);

		List<PlatformDimension> platFormDemision = PlatformDimension.buildList(platform);

		StatsCommonDimension statsCommon = statsUserDimension.getStatsCommon();
		statsCommon.setDate(dateDimensionList);
		for (PlatformDimension fl : platFormDemision) {
			statsCommon.setKpi(activeUserKpi);
			statsCommon.setPlatform(fl);
			statsUserDimension.setBrowser(defaultBrowser);
			// 设置kpi dimension
			context.write(this.statsUserDimension, this.timeOutputValue);

			for (BrowserDimension bw : browsers) {
				this.statsUserDimension.setBrowser(bw); // 设置对应的browsers
				statsCommon.setKpi(activeUserOfBrowserKpi);
				context.write(this.statsUserDimension, this.timeOutputValue);
			}

		}

	}

}
