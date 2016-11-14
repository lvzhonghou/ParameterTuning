package com.lvzhonghou.common;
import java.util.List;


/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016年2月28日 下午8:21:55 
 * @version v1。0
 */
public class ReduceTraceData {
   public long initialCost;   //初始化时间
   
   public int memoryLimit;   //memory capacity for shuffle
   public int maxSingleShuffleLimit;  //内存中容纳单个mapoutput的最大阀值
   public int mergeThreshold;   //内存合并的阀值
   public int ioSortFactor;    //磁盘文件合并的文件数
   public double maxInMemCopyUse;   //内存用于存储mapoutput的比率
   public double singleShuffleMemoryLimitPercent;   //内存中容纳最大的mapoutput的比例
   public List<ShuffleInfo> shuffles;  //所有shuffles的信息
   public List<MemMergeInShuffle> memMergesInShu;
   public List<DiskMergeInShuffle> diskMergesInShu;
   public double reduceInBufPerc;   //reduce时，内存中供存放mapoutput的空间比例
   public int maxInMemReduce;   //reduce时，内存中供存放mapoutput的空间
   public FinalMerge finalMerge;  //shuffle完之后，finalMerge
   public long reduceCost;   //reduce开销  
   public long hdfsWriteCost;   //hdfs写开销
   public long numsReduce;    //reduce执行次数
   public long reduceOutSize;//hdfs写的数据大小
}

