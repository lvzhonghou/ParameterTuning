package com.lvzhonghou.common;
import java.io.Serializable;


/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016年3月2日 下午4:43:24 
 * @version v1。0
 */
public class ReduceDataFlowStatistics implements Serializable{
    private static final long serialVersionUID = -5240152692961888098L;
  //  double dsMemToDiskCombineRecsSelect;  //将内存的数据写入disk，combine前后kv数目的转化率
    
    public double dsShuffleMemToDiskCombineSizeSelect;
    public double dsShuffleMemToDiskCombineRecsSelect;
    
    public double dsMemToDiskCombineSizeSelect;   //combine前后，size的转化率
    public double dsMemToDiskCombineRecsSelect;  //combine前后，recs的转化率
    
    public double dsReduceNumSelect;   // 用来计算reduce次数
    public double dsReduceSizeSelect;  //执行reduce函数后 ,输出、输入数据量的变化
    
}
