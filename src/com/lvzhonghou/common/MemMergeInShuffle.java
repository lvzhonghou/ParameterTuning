package com.lvzhonghou.common;

/** 
 * @Description  
 * @author zhonghou.lzh
 * @date 2016年7月12日 上午11:05:24 
 * @version v1。0
 */
public class MemMergeInShuffle {  //在shuffle阶段的merge
   public long mergeCost;
   public long writeCost;
   public int mergeSize;
   public boolean haveCombine;
}
