package com.lvzhonghou.StatisticsEstimate;

import com.lvzhonghou.common.MapOrReduce;

/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016年7月23日 下午4:22:55 
 * @version v1。0
 */
public class Parameter {
    public String paraName;
    public MapOrReduce mapOrReduce;
    public boolean isTradeOff; // the true represent the non-linearity effect on the execution cost
    public boolean isActive;

    public Parameter() {
	
    }
    
    public Parameter(String paraName, MapOrReduce mapOrReduce, boolean isTradeOff, boolean isActive) {
	this.paraName = paraName;
	this.mapOrReduce = mapOrReduce;
	this.isTradeOff = isTradeOff;
	this.isActive = isActive;
    }
}
