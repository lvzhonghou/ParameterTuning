package com.lvzhonghou.common;

/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016��2��29�� ����2:51:48 
 * @version v1��0
 */

public class ShuffleInfo {
    public String fetchThread;  //fetch�߳���
    public String mapTaskId;  //shuffle��Ŀ��mapTask
    public String hostId;   //hostId
    public int compressLength;  //comLen
    public int decomLength;   //decomLength
    public long startShuffle;   //the start time of shuffle
    public long endShuffle;   //the end time of shuffle
    public long shuffleCost;   // the cost time of shuffle
    public ShuffleType type;  // shuffle type
}

