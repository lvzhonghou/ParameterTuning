package com.lvzhonghou.common;
import java.io.Serializable;
import java.util.ArrayList;


/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016年1月17日 下午5:14:04 
 * @version v1。0
 */
public class MapCostStatistics implements Serializable {
    private static final long serialVersionUID = -5240152692961888094L;
    
    public double csScheduleCost;  //map task的调度开销
    
    public double csInitialCost;   //初始化开销
    public double hdfsReadRate = 100;   //hdfs读取速度  MB/Sec
    public double ioReadRate = 130;   //本地io读取速度
    public double csReadCost;  //读数据开销
    public double csMapCost;  //执行map函数开销(没有spill线程执行下的map开销)
    public double csMapCostWithSpill;  //执行map函数开销(在spill线程执行的同时，map函数的开销)
    
    public double csSortCost;  //排序开销
    public double csCombineCost;  //combine开销
    public double csCombineReadCost;    //读缓冲区的开销，对缓冲区的数据进行反序列化的开销
    public double csCombineWriteCost;  //序列化写的开销
    
    public double csMergeReadCost;   //merge读取文件、反序列化开销
    public double csMergeCombineCost;   //merge combine开销
    public double csMergeWriteCost;   //merge序列化、写文件

}
