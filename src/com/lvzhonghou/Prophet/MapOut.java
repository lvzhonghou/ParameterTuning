package com.lvzhonghou.Prophet;

/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016��7��13�� ����9:49:24 
 * @version v1��0
 */
public class MapOut implements Comparable<MapOut> {
    double startTime = 0; // �����һ��mapOut��ʱ��
    double clock; // ��¼ʱ�������diskOut��д���ʱ�䣩
    int nodeId; // ��¼��MapOut������ statistics
    int nums; // ��ǰ������ʣ��mapoutput����
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
