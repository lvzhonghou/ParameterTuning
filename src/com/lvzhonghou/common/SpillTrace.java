package com.lvzhonghou.common;

/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016年1月16日 下午10:52:14 
 * @version v1。0
 */
public class SpillTrace {
   public long mapToSpill; // map开始执行到spill开始
   public long mainThreadSleep; // 主线程休眠时间
   public long spillTime; // 整个spill时间
   public long sortTime; // 排序时间
   public long combineAndWrite; // combineAndWrite时间
   public long readTime = 0; // read时间
   public long writeTime = 0; // write时间
   public int spillOutFile; // spill输出文件大小
   public int kvNums; // 当前spill中需要处理的key、value数目
   public int partitionNums;  //partition的个数，每个partition都会有一次combine
   public boolean haveCombiner = true;
   public int combineOutRecs = 0; // combine后输出的records数
}