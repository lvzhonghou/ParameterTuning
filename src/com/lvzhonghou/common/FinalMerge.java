package com.lvzhonghou.common;

/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016年7月12日 下午3:47:30 
 * @version v1。0
 */
public class FinalMerge {
   public int numMemMapOutputs;  //内存中mapoutputs个数  
   public int numDiskMapOutputs; //磁盘中文件个数
   public boolean isFinalMerge;   //是否最后一轮合并
   public long memToDiskSize;  //将内存中memToDiskSize大小合并到磁盘文件中
   public boolean isCleanMemToDisk = true;  //是否需要将内存的部分数据合并写入磁盘中,清空部分数据
   public boolean isFirstInmeMerge = true;  //是否需要进行第一次中间合并
   public boolean haveCombiner; //是否定义combiner
   public long memToDiskMergeCost;  //将内存中合并的开销、序列化与反序列化的开销
   public long memToDiskWriteCost;   //将内存合并后写入磁盘的开销
   
   public long immeFirstImmeMergeCost = 0; //第一次合并
   public long immeFirstImmeWriteCost = 0;
   public long immeFirstImmeSize = 0;
    
   public long immeNotFirstImmeMergeCost;
   public long immeNotFirstImmeWriteCost;
   public long immeNotFirstImmeSize;
    
   public long finalMemSize;   //最后合并内存数据的大小
   public long finalDiskSize;   //最后合并磁盘数据的大小
   public long finalMergeSize;  //最后合并总的数据大小，finalMergeSize = finalDiskSize + finalMemSize
   public long finalMergeCost;   //最后合并的开销
}
