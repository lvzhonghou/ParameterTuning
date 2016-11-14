package com.lvzhonghou.StatisticsEstimate;

import com.lvzhonghou.common.MapOrReduce;
import com.lvzhonghou.common.StatisticsType;

/**
 * @Description
 * @author zhonghou.lzh
 * @date 2016年7月23日 下午4:23:06
 * @version v1。0
 */
public class Statistic {
    public String name;
    public MapOrReduce mapOrReduce;
    public StatisticsType statType;
    public boolean isActive;

    public Statistic(String name, MapOrReduce mapOrReduce,
	    StatisticsType statType, boolean isActive) {
	this.name = name;
	this.mapOrReduce = mapOrReduce;
	this.statType = statType;
	this.isActive = isActive;
    }
}
