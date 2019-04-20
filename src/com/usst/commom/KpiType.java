package com.usst.commom;

/**
 * 统计kpi的名称枚举类
 * 
 * @author root
 * 
 * */
public enum KpiType {
	NEW_INSTALL_USER("new_install_user"),  //统计新用户的kpi
	
	BROWSER_NEW_INSTALL_USER("browser_new_install_user"),  //统计浏览器新用户的kpi
	
	ACTIVE_USER("active_user"), //统计活跃用户是指标
	
	BROWSER_ACTIVE_USER("browser_active_user"),  //统计浏览器维度的活跃用户是指标
	;
	
	public final String name;
	
	private KpiType(String name) {
		this.name = name;
	}
	
	//根据kpitype的名称字符串值，获取对应的kpitype的枚举对象
	public static KpiType valueOfName(String name) {
		for (KpiType type : values()) {
			if (type.name.equals(name)) {
				return type;
			}
		}
		throw new RuntimeException("指定的name不属于该KpiType枚举类：" + name);
	}
	
	
	
	

}
