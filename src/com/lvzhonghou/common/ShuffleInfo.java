package com.lvzhonghou.common;

/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016年2月29日 下午2:51:48 
 * @version v1。0
 */

public class ShuffleInfo {
    public String fetchThread;  //fetch线程名
    public String mapTaskId;  //shuffle的目标mapTask
    public String hostId;   //hostId
    public int compressLength;  //comLen
    public int decomLength;   //decomLength
    public long startShuffle;   //the start time of shuffle
    public long endShuffle;   //the end time of shuffle
    public long shuffleCost;   // the cost time of shuffle
    public ShuffleType type;  // shuffle type
}

