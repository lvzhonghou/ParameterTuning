package com.lvzhonghou.Prophet;
import java.io.Serializable;
import java.util.List;


/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016年3月4日 下午7:45:51 
 * @version v1。0
 */
public class ReduceTaskParameters implements Serializable {
    /** @Fields serialVersionUID: */
    private static final long serialVersionUID = -8516350778084476378L;
    
    public double pTotalMemorySize;  //memoryLimit 
   // double pMaxInMemCopyUse;  //内存用于存储mapoutput的比率(0.7)
    public double pSingleShuffleMemoryLimitPercent;  //内存中容纳最大的mapoutput的比例 (0.25)
    public double pMergeThresholdPercent;  //mergeThreshold / memoryLimit (0.9)
    public double pSortFactor;  //磁盘文件单次合并的文件数(2 * pSortFactor - 1)
    public double pCopyThread; //拷贝线程的个数
    public double pReduceInBufPerc;  //reduce时，内存中供存放mapoutput的空间比例
    public double pNumReduce;  //reduceTask的个数
    public boolean pHaveCombiner = false; //memToDisk是否需要进行combine操作
}