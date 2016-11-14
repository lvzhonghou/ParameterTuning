package com.lvzhonghou.Prophet;

/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016年7月13日 上午9:49:24 
 * @version v1。0
 */
public class MapOut implements Comparable<MapOut> {
    double startTime = 0; // 距离第一个mapOut的时间
    double clock; // 记录时间戳（如diskOut的写完成时间）
    int nodeId; // 记录该MapOut所属的 statistics
    int nums; // 当前机器中剩余mapoutput个数
    double mapCost;
    double mapScheduleCost;
    double mapOutSize;
    double mapOutRecs;
    
    public MapOut() {

    }

    public MapOut(int nums, double mapCost, double mapScheduleCost, double mapOutSize, double mapOutRecs, int nodeId) {
	this.nums = nums;
	this.mapCost = mapCost;
	this.mapScheduleCost = mapScheduleCost;
	this.mapOutSize = mapOutSize;
	this.mapOutRecs = mapOutRecs;
	this.nodeId = nodeId;
    }
    
    public MapOut(double mapOutSize, double mapOutRecs) {
	this.mapOutSize = mapOutSize;
	this.mapOutRecs = mapOutRecs;
    }
    
    @Override
    public int compareTo(MapOut mapOut) {
	// TODO Auto-generated method stub
	if (this.mapCost + this.mapScheduleCost > mapOut.mapCost + mapOut.mapScheduleCost)
	    return 1;
	else
	    return -1;
    }
    
    public double getMapCost() {
	return this.mapCost;
    }
    
    public int getNodeId() {
	return this.nodeId;
    }
}
