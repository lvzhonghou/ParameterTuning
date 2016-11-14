package com.lvzhonghou.common;
import java.util.ArrayList;
import java.util.List;


/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016年1月16日 下午10:57:09 
 * @version v1。0
 */
public class MapTraceData {
   public long initialCost; // 初始化开销
   public int sortMB; // sortMB大小
   public int sortLimit; // sortLimit大小
   public int setUpCost; // setup开销
   public ArrayList<SpillTrace> spillList = new ArrayList<SpillTrace>();
   public long mapCost; // map函数执行的开销
   public int maps; // map函数循环执行的次数
   public int cleanUp; // cleanup开销
   public long inputFileSize; // mapTask处理的输入文件大小
   public long readAndMapCost; // 读取split数据和执行map的开销
   public int sortFactor; // 指定的sortFactor
   public int numSpillForComb; // merge时进行combine的spill数量阀值
   public List<MergeTrace> mergeList = new ArrayList<MergeTrace>();
   public long mapTaskCost; // 整个map任务的开销
   public int mapOutBytes; // map函数所有输出结果的字节数和
   public int mapOutRecs; // map函数所有的输出结果的记录数
    
   public int mapOutRecsWithSpill; //在spill的同时，map函数输出的记录数
   public long mapCostWithSpill;  //在spill的同时，map函数执行的总开销
}

