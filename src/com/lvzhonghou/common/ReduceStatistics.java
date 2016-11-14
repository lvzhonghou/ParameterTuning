package com.lvzhonghou.common;
import java.io.Serializable;

/**
 * @Description
 * @author zhonghou.lzh
 * @date 2016年3月2日 下午4:45:06
 * @version v1。0
 */
public class ReduceStatistics implements Serializable {
    private static final long serialVersionUID = -5240152692961888099L;
    public ReduceCostStatistics reduceCostStatistics;
    public ReduceDataFlowStatistics reduceDataFlowStatistics;

    public ReduceStatistics() {
	reduceCostStatistics = new ReduceCostStatistics();
	reduceDataFlowStatistics = new ReduceDataFlowStatistics();
    }
}
