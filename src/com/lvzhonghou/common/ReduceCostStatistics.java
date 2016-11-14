package com.lvzhonghou.common;
import java.io.Serializable;


/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016年3月2日 下午4:42:53 
 * @version v1。0
 */
public class ReduceCostStatistics implements Serializable{
    private static final long serialVersionUID = -5240152692961888097L;
    
    public double csSheduleCost;  //reduce task的调度开销
    
    public double initialCost;   //初始化
    
    public double csShuffleInMem;   //cs of shuffle in mem
    public double csShuffleOnDisk;   //cs of shuffle on disk
    
    public double csShuffleMemToDiskMerge;   //cs of immediate merge the kv from the mem to disk when in shuffle
    public double csShuffleMemToDiskCombine;  //cs of immediate combine
    public double csShuffleMemToDiskWrite;   //cs of imediate write the kv from the mem to disk
    
    public double csShuffleDiskToDiskMerge;     //cs of imediate merge the kv from the disk to disk
    public double csShuffleDiskToDiskWrite;    //cs of imediate write the kv from the disk to disk
    
    public double csImeMemToDiskMerge;   //cs of immediate merge the kv from the mem to disk
    public double csImeMemToDiskCombine;   //cs of immediate combine
    public double csImeMemToDiskWrite;  //cs of imediate write the kv from the mem to disk
    
    public double csImeDiskToDiskMerge;   //
    public double csImeDiskToDiskWrite;
    
    public double csFinalMemToDiskMerge;   //cs of final merge the kv from the mem to disk
    public double csFinalMemToDiskWrite;  //cs of final write the kv from the mem to disk
    public double csFinalDiskToDiskMerge;  //cs of final merge the kv from the disk to disk
    
    public double csReduce;   
    public double csHdfsWrite;  //hdfs写开销
}
