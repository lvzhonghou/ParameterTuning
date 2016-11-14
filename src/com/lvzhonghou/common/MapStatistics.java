package com.lvzhonghou.common;
import java.io.Serializable;


/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016年1月17日 下午10:09:44 
 * @version v1。0
 */
public class MapStatistics implements Serializable{
    private static final long serialVersionUID = -5240152692961888096L;
    public MapDataFlowStatistics dataFlowStat;
    public MapCostStatistics costStat;
    
    public MapStatistics() {
	dataFlowStat = new MapDataFlowStatistics();
	costStat = new MapCostStatistics();
    }
}
